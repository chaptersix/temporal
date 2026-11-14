package scheduler2

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

var operationReadyIndex = queueindex.SequenceIndex{1, 'r', 'e', 'a', 'd', 'y'}

type operationReadyEntry struct {
	Operation            *pb.OperationState
	Sequence             uint64
	LogicalWake, ReadyAt int64
}
type operationReadyChange struct {
	sequence uint64
	record   *pb.ReadyRecord
}
type operationReadinessPlan struct {
	indexes       operationIndexPlan
	records       []operationReadyChange
	recovered     []*pb.OperationState
	ClaimReleased bool
}

// operationReadinessEditor owns a single unpublished index/locator overlay and
// the caller's detached prospective control. The initial token and next unused
// ready sequence are both one. Token/armed reconciliation belongs
// to the dispatcher and must precede publication of that same control object.
type operationReadinessEditor struct {
	scheduler     *Scheduler
	ctx           chasm.Context
	control       *pb.OperationStoreControl
	capture       *operationIndexCapture
	batch         *queueindex.CountBatch
	records       map[uint64]*pb.ReadyRecord
	metadata      map[string]*pb.OperationState
	recovered     map[string]*pb.OperationState
	claimReleased bool
	err           error
	closed        bool
}

func readyRecordKey(sequence uint64) string { return fmt.Sprintf("%016x", sequence) }
func operationWake(op *pb.OperationState) (int64, bool) {
	if op == nil {
		return 0, false
	}
	return decision.Wake(decision.State{Operation: &decision.Operation{Phase: decision.Phase(op.Phase), RetryAt: op.RetryAt, Deadline: op.Deadline}})
}
func readyTimeKey(at int64) queueindex.CountKey {
	key := queueindex.NewTimeKey(time.Unix(0, at), 0)
	return queueindex.CountKey(key[:12])
}

func (s *Scheduler) newOperationReadinessEditor(ctx chasm.Context, control *pb.OperationStoreControl) (*operationReadinessEditor, error) {
	if err := s.validateOperationStore(); err != nil {
		return nil, err
	}
	if s.OperationStore == nil || s.OperationStore.StartDispatch == nil || control == nil || control.StartDispatch == nil {
		return nil, serviceerror.NewInternal("readiness requires initialized operation dispatch")
	}
	if control.PendingCount != s.OperationStore.PendingCount || control.NextReadySequence != s.OperationStore.NextReadySequence || !proto.Equal(control.StartDispatch, s.OperationStore.StartDispatch) {
		return nil, serviceerror.NewInternal("stale readiness control")
	}
	capture := &operationIndexCapture{reader: s.operationIndexReader(ctx, false)}
	e := &operationReadinessEditor{scheduler: s, ctx: ctx, control: control, capture: capture, batch: queueindex.NewCountStoreWithBackend(capture).Begin(), records: make(map[uint64]*pb.ReadyRecord), metadata: make(map[string]*pb.OperationState), recovered: make(map[string]*pb.OperationState)}
	if err := e.validateCount(s.OperationStore); err != nil {
		e.batch.Abort()
		return nil, err
	}
	if control.StartDispatch.Claim != nil {
		if _, err := e.claimEntry(); err != nil {
			e.batch.Abort()
			return nil, err
		}
	}
	return e, nil
}
func (e *operationReadinessEditor) ready() error {
	if e.err != nil {
		return e.err
	}
	if e.closed {
		return serviceerror.NewInternal("readiness editor closed")
	}
	return nil
}
func (e *operationReadinessEditor) fail(err error) {
	if err != nil && e.err == nil {
		e.err = err
	}
}
func (e *operationReadinessEditor) abort() { e.closed = true; e.batch.Abort() }

func (e *operationReadinessEditor) validateCount(control *pb.OperationStoreControl) error {
	if control.PendingCount < 0 || control.PendingCount > control.OperationCount || control.NextReadySequence == 0 || control.StartDispatch == nil || control.StartDispatch.Token == 0 {
		return serviceerror.NewInternal("invalid readiness summary")
	}
	expected := control.PendingCount
	if claim := control.StartDispatch.Claim; claim != nil {
		if expected == 0 || claim.OperationId == "" || claim.ReadySequence == 0 || claim.ReadySequence >= control.NextReadySequence {
			return serviceerror.NewInternal("invalid readiness claim")
		}
		expected--
	}
	count, err := e.batch.SequenceCount(operationReadyIndex, 0, math.MaxUint64, queueindex.CountKey{})
	if err != nil {
		return err
	}
	if count != uint64(expected) {
		return serviceerror.NewInternal("readiness index count disagrees with summary")
	}
	return nil
}
func (e *operationReadinessEditor) operation(id string) (*pb.OperationState, error) {
	op, exists := e.metadata[id]
	if !exists {
		var err error
		op, exists, err = e.scheduler.operationRecord(e.ctx, id)
		if err != nil {
			return nil, err
		}
	}
	if !exists || op == nil || op.Id != id {
		return nil, serviceerror.NewInternal("readiness operation record is missing or mismatched")
	}
	return common.CloneProto(op), nil
}
func (e *operationReadinessEditor) locator(sequence uint64) (*pb.ReadyRecord, error) {
	record, exists := e.records[sequence]
	if !exists {
		var err error
		record, exists, err = e.scheduler.readyRecord(e.ctx, sequence)
		if err != nil {
			return nil, err
		}
	}
	if !exists || record == nil {
		return nil, serviceerror.NewInternal("readiness locator is missing")
	}
	return common.CloneProto(record), nil
}
func (e *operationReadinessEditor) entry(op *pb.OperationState) (*operationReadyEntry, error) {
	wake, pending := operationWake(op)
	if !pending || op.ReadySequence == 0 || op.ReadySequence >= e.control.NextReadySequence {
		return nil, serviceerror.NewInternal("invalid readiness operation membership")
	}
	record, err := e.locator(op.ReadySequence)
	if err != nil {
		return nil, err
	}
	if record.OperationId != op.Id || record.LogicalWake != wake {
		return nil, serviceerror.NewInternal("readiness locator disagrees with operation")
	}
	indexed, present, err := e.batch.Sequence(operationReadyIndex, op.ReadySequence)
	if err != nil {
		return nil, err
	}
	claim := e.control.StartDispatch.Claim
	claimed := claim != nil && claim.OperationId == op.Id
	if claimed {
		if claim.ReadySequence != op.ReadySequence || claim.LogicalWake != wake || claim.Expiry <= record.ReadyAt || present {
			return nil, serviceerror.NewInternal("readiness claim disagrees with membership")
		}
	} else if !present || indexed.Time != readyTimeKey(record.ReadyAt) {
		return nil, serviceerror.NewInternal("readiness index disagrees with locator")
	}
	return &operationReadyEntry{Operation: common.CloneProto(op), Sequence: op.ReadySequence, LogicalWake: wake, ReadyAt: record.ReadyAt}, nil
}
func (e *operationReadinessEditor) claimEntry() (*operationReadyEntry, error) {
	claim := e.control.StartDispatch.Claim
	if claim == nil {
		return nil, serviceerror.NewInternal("readiness claim is absent")
	}
	op, err := e.operation(claim.OperationId)
	if err != nil {
		return nil, err
	}
	return e.entry(op)
}

func (e *operationReadinessEditor) minimum() (result *operationReadyEntry, err error) {
	defer func() { e.fail(err) }()
	if err = e.ready(); err != nil {
		return nil, err
	}
	if err = e.validateCount(e.control); err != nil {
		return nil, err
	}
	indexed, present, err := e.batch.SequenceEarliest(operationReadyIndex)
	if err != nil || !present {
		return nil, err
	}
	locator, err := e.locator(indexed.Sequence)
	if err != nil {
		return nil, err
	}
	op, err := e.operation(locator.OperationId)
	if err != nil {
		return nil, err
	}
	entry, err := e.entry(op)
	if err != nil {
		return nil, err
	}
	if entry.Sequence != indexed.Sequence || indexed.Time != readyTimeKey(entry.ReadyAt) {
		return nil, serviceerror.NewInternal("readiness minimum disagrees with locator")
	}
	return entry, nil
}

func (e *operationReadinessEditor) change(change operationRecordChange) (err error) {
	defer func() { e.fail(err) }()
	if err = e.ready(); err != nil {
		return err
	}
	if _, seen := e.metadata[change.id]; seen || len(e.metadata) >= 3 {
		return serviceerror.NewInternal("readiness record mutation budget exceeded")
	}
	old, exists, err := e.scheduler.operationRecord(e.ctx, change.id)
	if err != nil {
		return err
	}
	if exists && (old == nil || old.Id != change.id) {
		return serviceerror.NewInternal("invalid old readiness operation")
	}
	if change.operation != nil && change.operation.Id != change.id {
		return serviceerror.NewInternal("invalid new readiness operation")
	}
	oldWake, wasPending := operationWake(old)
	newWake, pending := operationWake(change.operation)
	if exists && !wasPending && old.ReadySequence != 0 {
		return serviceerror.NewInternal("settled operation retains readiness sequence")
	}
	if wasPending {
		if _, err = e.entry(old); err != nil {
			return err
		}
	}
	if wasPending && pending && oldWake == newWake {
		change.operation.ReadySequence = old.ReadySequence
		e.metadata[change.id] = common.CloneProto(change.operation)
		return nil
	}
	if wasPending {
		if err = e.remove(old); err != nil {
			return err
		}
	}
	if change.operation != nil {
		change.operation.ReadySequence = 0
	}
	if pending {
		if err = e.add(change.operation, newWake); err != nil {
			return err
		}
	}
	e.metadata[change.id] = common.CloneProto(change.operation)
	return nil
}
func (e *operationReadinessEditor) remove(op *pb.OperationState) error {
	if _, err := e.entry(op); err != nil {
		return err
	}
	if e.control.PendingCount <= 0 {
		return serviceerror.NewInternal("readiness count underflow")
	}
	if claim := e.control.StartDispatch.Claim; claim != nil && claim.OperationId == op.Id {
		e.control.StartDispatch.Claim = nil
		e.claimReleased = true
	} else if err := e.batch.DeleteSequence(operationReadyIndex, op.ReadySequence); err != nil {
		return err
	}
	e.records[op.ReadySequence] = nil
	e.control.PendingCount--
	return nil
}
func (e *operationReadinessEditor) add(op *pb.OperationState, readyAt int64) error {
	if e.control.NextReadySequence == 0 || e.control.NextReadySequence == math.MaxUint64 {
		return serviceerror.NewInternal("readiness sequence exhausted")
	}
	if e.control.PendingCount == math.MaxInt64 {
		return serviceerror.NewInternal("readiness count exhausted")
	}
	wake, pending := operationWake(op)
	if !pending {
		return serviceerror.NewInternal("readiness requires pending operation")
	}
	sequence := e.control.NextReadySequence
	if _, exists := e.records[sequence]; exists {
		return serviceerror.NewInternal("readiness sequence was reused")
	}
	if _, exists, err := e.scheduler.readyRecord(e.ctx, sequence); err != nil {
		return err
	} else if exists {
		return serviceerror.NewInternal("readiness locator sequence collision")
	}
	if err := e.batch.InsertSequence(operationReadyIndex, sequence, readyTimeKey(readyAt)); err != nil {
		return err
	}
	op.ReadySequence = sequence
	e.records[sequence] = &pb.ReadyRecord{OperationId: op.Id, LogicalWake: wake, ReadyAt: readyAt}
	e.control.NextReadySequence++
	e.control.PendingCount++
	return nil
}

// claim removes exactly the earliest due membership while retaining its locator.
// The dispatcher owns the token change and expiry's physical-deadline conversion.
func (e *operationReadinessEditor) claim(selected *operationReadyEntry, now, expiry int64) (err error) {
	defer func() { e.fail(err) }()
	if err = e.ready(); err != nil {
		return err
	}
	if e.control.StartDispatch.Claim != nil || expiry <= now {
		return serviceerror.NewInternal("invalid readiness claim acquisition")
	}
	current, err := e.minimum()
	if err != nil {
		return err
	}
	if selected == nil || current == nil || selected.Sequence != current.Sequence || selected.Operation == nil || selected.Operation.Id != current.Operation.Id || selected.LogicalWake != current.LogicalWake || selected.ReadyAt != current.ReadyAt {
		return serviceerror.NewInternal("stale readiness selection")
	}
	if current.ReadyAt > now || now >= current.Operation.Deadline {
		return serviceerror.NewInternal("readiness selection is not issuable")
	}
	if err = e.batch.DeleteSequence(operationReadyIndex, current.Sequence); err != nil {
		return err
	}
	e.control.StartDispatch.Claim = &pb.StartClaim{OperationId: current.Operation.Id, ReadySequence: current.Sequence, LogicalWake: current.LogicalWake, Expiry: expiry}
	return nil
}

// recover requeues a pending claim with a fresh identity without changing its
// logical retry/deadline. Expired issuance must first use the reducer's Expire.
func (e *operationReadinessEditor) recover(now int64) (op *pb.OperationState, err error) {
	defer func() { e.fail(err) }()
	if err = e.ready(); err != nil {
		return nil, err
	}
	current, err := e.claimEntry()
	if err != nil {
		return nil, err
	}
	if now < e.control.StartDispatch.Claim.Expiry {
		return nil, serviceerror.NewInternal("readiness claim recovery is not due")
	}
	if now >= current.Operation.Deadline {
		return nil, serviceerror.NewInternal("expired readiness claim requires operation expiry")
	}
	if e.control.NextReadySequence == math.MaxUint64 {
		return nil, serviceerror.NewInternal("readiness sequence exhausted")
	}
	op = common.CloneProto(current.Operation)
	if err = e.remove(op); err != nil {
		return nil, err
	}
	if err = e.add(op, now); err != nil {
		return nil, err
	}
	e.metadata[op.Id] = common.CloneProto(op)
	e.recovered[op.Id] = common.CloneProto(op)
	return common.CloneProto(op), nil
}

func (e *operationReadinessEditor) finish() (plan operationReadinessPlan, err error) {
	defer func() { e.fail(err) }()
	if err = e.ready(); err != nil {
		return plan, err
	}
	if err = e.validateCount(e.control); err != nil {
		return plan, err
	}
	if err = e.batch.Commit(); err != nil {
		return plan, err
	}
	e.closed = true
	plan.indexes = operationIndexPlan{changes: e.capture.changes}
	plan.ClaimReleased = e.claimReleased
	for sequence, record := range e.records {
		plan.records = append(plan.records, operationReadyChange{sequence: sequence, record: common.CloneProto(record)})
	}
	slices.SortFunc(plan.records, func(a, b operationReadyChange) int { return cmp.Compare(a.sequence, b.sequence) })
	for _, op := range e.recovered {
		plan.recovered = append(plan.recovered, common.CloneProto(op))
	}
	slices.SortFunc(plan.recovered, func(a, b *pb.OperationState) int { return cmp.Compare(a.Id, b.Id) })
	return plan, nil
}

// prepareOperationReadiness receives detached operation changes and the same
// prospective control used by retirement/record preflight, never persisted values.
func (s *Scheduler) prepareOperationReadiness(ctx chasm.Context, changes []operationRecordChange, control *pb.OperationStoreControl) (operationReadinessPlan, error) {
	if s.OperationStore == nil {
		return operationReadinessPlan{}, nil
	}
	if len(changes) > 3 {
		return operationReadinessPlan{}, serviceerror.NewInternal("readiness mutation budget exceeded")
	}
	e, err := s.newOperationReadinessEditor(ctx, control)
	if err != nil {
		return operationReadinessPlan{}, err
	}
	defer e.abort()
	seen := make(map[string]bool, len(changes))
	for _, change := range changes {
		if change.id == "" || seen[change.id] {
			return operationReadinessPlan{}, serviceerror.NewInternal("invalid readiness mutation identity")
		}
		seen[change.id] = true
		if err = e.change(change); err != nil {
			return operationReadinessPlan{}, err
		}
	}
	return e.finish()
}
func (s *Scheduler) publishOperationReadiness(ctx chasm.MutableContext, plan operationReadinessPlan) {
	if s.PackedStorage != nil {
		return
	}
	s.publishOperationIndexes(ctx, plan.indexes)
	for _, change := range plan.records {
		if change.record == nil {
			s.ReadyRecords.Delete(ctx, readyRecordKey(change.sequence))
		} else {
			s.ReadyRecords.Put(ctx, readyRecordKey(change.sequence), change.record)
		}
	}
	for _, op := range plan.recovered {
		s.OperationRecords.Put(ctx, op.Id, op)
	}
}
func (s *Scheduler) readOperationReady(ctx chasm.Context) (*operationReadyEntry, error) {
	e, err := s.newOperationReadinessEditor(ctx, common.CloneProto(s.OperationStore))
	if err != nil {
		return nil, err
	}
	defer e.abort()
	return e.minimum()
}
