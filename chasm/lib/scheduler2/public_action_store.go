package scheduler2

import (
	"fmt"
	"math"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

const projectionStoreVersion = 1

var runningProjectionIndex = queueindex.SequenceIndex{1, 'p', 'u', 'b', 'l', 'i', 'c', 'r', 'u', 'n'}

type projectionMutation struct {
	control      *pb.ProjectionStoreControl
	actions      map[string]*pb.PublicActionRecord
	running      map[string]*pb.RunningPublicAction
	indexes      []queueindex.IndexChange
	recentVictim string
}

func runningRecordKey(sequence uint64) string { return fmt.Sprintf("%016x", sequence) }

func (s *Scheduler) validatePublicProjection(ctx chasm.Context) error {
	if s.ProjectionStore == nil {
		return nil
	}
	c := s.ProjectionStore
	if c.FormatVersion != projectionStoreVersion || c.NextActionSequence == 0 || c.RunningCount < 0 {
		return serviceerror.NewInternal("invalid public projection control")
	}
	runtime, ok := s.PublicRuntime.TryGet(ctx)
	if !ok && (c.NextActionSequence != 1 || c.RunningCount != 0) {
		return serviceerror.NewInternal("public projection runtime is missing")
	}
	if ok {
		if len(runtime.Actions) != 0 || len(runtime.CompletedOperations) > 10 || len(runtime.RecentActions) > 10 {
			return serviceerror.NewInternal("invalid public projection representation")
		}
		if err := validateCompletedActions(runtime.CompletedOperations); err != nil {
			return err
		}
		if err := validateRecentActions(runtime.RecentActions, c.NextActionSequence); err != nil {
			return err
		}
	}
	batch := queueindex.NewCountStoreWithBackend(&indexBackend{ctx: ctx, pages: s.ProjectionPages}).Begin()
	defer batch.Abort()
	return validateRunningCount(batch, c.RunningCount)
}
func validateCompletedActions(completed []*pb.PublicCompletedOperation) error {
	seen := make(map[string]bool, len(completed))
	for i, entry := range completed {
		if entry == nil || entry.OperationId == "" || seen[entry.OperationId] || entry.CloseTime == nil || entry.CloseTime.CheckValid() != nil {
			return serviceerror.NewInternal("invalid completed public action")
		}
		if i > 0 && completed[i-1].CloseTime.AsTime().After(entry.CloseTime.AsTime()) {
			return serviceerror.NewInternal("invalid completed public action ordering")
		}
		seen[entry.OperationId] = true
	}
	return nil
}

func validateRunningCount(batch *queueindex.CountBatch, want int64) error {
	count, err := batch.SequenceCount(runningProjectionIndex, 0, math.MaxUint64, queueindex.CountKey{})
	if err != nil {
		return err
	}
	if want < 0 || count != uint64(want) {
		return serviceerror.NewInternal("running projection count mismatch")
	}
	return nil
}
func validateRecentActions(recent []*pb.RecentPublicAction, next uint64) error {
	ids := make(map[string]bool, len(recent))
	sequences := make(map[uint64]bool, len(recent))
	for i, entry := range recent {
		if entry == nil || entry.Sequence == 0 || entry.Sequence >= next || entry.Action == nil || entry.Action.OperationId == "" || entry.Action.Result.GetStartWorkflowResult().GetRunId() == "" || entry.Action.Result.GetActualTime() == nil || entry.Action.Result.ActualTime.CheckValid() != nil {
			return serviceerror.NewInternal("invalid recent public action")
		}
		if ids[entry.Action.OperationId] || sequences[entry.Sequence] || i > 0 && compareRecentActions(recent[i-1], entry) >= 0 {
			return serviceerror.NewInternal("invalid recent public action ordering")
		}
		ids[entry.Action.OperationId] = true
		sequences[entry.Sequence] = true
	}
	return nil
}
func compareRecentActions(a, b *pb.RecentPublicAction) int {
	if cmp := a.Action.Result.ActualTime.AsTime().Compare(b.Action.Result.ActualTime.AsTime()); cmp != 0 {
		return cmp
	}
	if a.Sequence < b.Sequence {
		return -1
	}
	if a.Sequence > b.Sequence {
		return 1
	}
	return 0
}
func recentContains(runtime *pb.PublicRuntime, id string) bool {
	return slices.ContainsFunc(runtime.RecentActions, func(a *pb.RecentPublicAction) bool { return a.Action.OperationId == id })
}
func updateRecentActions(runtime *pb.PublicRuntime, record *pb.PublicActionRecord) string {
	if record.Action.Result.GetStartWorkflowResult().GetRunId() == "" {
		return ""
	}
	previous := runtime.RecentActions
	next := make([]*pb.RecentPublicAction, 0, 11)
	for _, entry := range previous {
		if entry.Action.OperationId != record.Action.OperationId {
			next = append(next, entry)
		}
	}
	next = append(next, &pb.RecentPublicAction{Sequence: record.Sequence, Action: common.CloneProto(record.Action)})
	slices.SortFunc(next, compareRecentActions)
	if len(next) > 10 {
		next = next[1:]
	}
	runtime.RecentActions = next
	for _, old := range previous {
		if !recentContains(runtime, old.Action.OperationId) {
			return old.Action.OperationId
		}
	}
	return ""
}

func (s *Scheduler) finalizePublicTransition(ctx chasm.Context, plan publicTransitionPlan, records operationRecordPlan) (publicTransitionPlan, error) {
	if plan.projection == nil {
		return plan, nil
	}
	p := plan.projection
	capture := &operationIndexCapture{reader: &indexBackend{ctx: ctx, pages: s.ProjectionPages}}
	batch := queueindex.NewCountStoreWithBackend(capture).Begin()
	defer batch.Abort()
	for _, change := range records.changes {
		if err := s.prepareActionOwnership(ctx, &plan, change, batch); err != nil {
			return publicTransitionPlan{}, err
		}
	}
	if p.recentVictim != "" {
		record, known := p.actions[p.recentVictim]
		if !known {
			var ok bool
			record, ok = s.ActionRecords.Get(ctx, p.recentVictim)
			if !ok {
				return publicTransitionPlan{}, serviceerror.NewInternal("recent action record is missing")
			}
		}
		if record != nil && !record.OperationOwned {
			p.actions[p.recentVictim] = nil
		}
	}
	if len(p.actions) > 4 {
		return publicTransitionPlan{}, serviceerror.NewInternal("public action mutation budget exceeded")
	}
	if err := validateRunningCount(batch, p.control.RunningCount); err != nil {
		return publicTransitionPlan{}, err
	}
	if err := batch.Commit(); err != nil {
		return publicTransitionPlan{}, err
	}
	p.indexes = capture.changes
	return plan, nil
}

func (s *Scheduler) prepareActionOwnership(ctx chasm.Context, plan *publicTransitionPlan, change operationRecordChange, batch *queueindex.CountBatch) error {
	p := plan.projection
	old, existed := s.ActionRecords.Get(ctx, change.id)
	record, changed := p.actions[change.id]
	if !changed {
		record = common.CloneProto(old)
	}
	before, hadOperation := s.operationRecord(ctx, change.id)
	if record == nil {
		if hadOperation || change.operation != nil {
			return serviceerror.NewInternal("public action record is missing")
		}
		return nil
	}
	if err := validatePublicActionRecord(record, change.id, p.control.NextActionSequence); err != nil {
		return err
	}
	if existed && old.OperationOwned != hadOperation {
		return serviceerror.NewInternal("public action ownership mismatch")
	}
	wasRunning := hadOperation && before.Phase == pb.PHASE_STARTED && existed && old.Action.GetResult().GetStartWorkflowResult().GetRunId() != ""
	isRunning := change.operation != nil && change.operation.Phase == pb.PHASE_STARTED && record.Action.Result.GetStartWorkflowResult().GetRunId() != ""
	if wasRunning != isRunning {
		if err := s.prepareRunningMembership(ctx, p, record, old, change.id, wasRunning, isRunning, batch); err != nil {
			return err
		}
	}
	record.OperationOwned = change.operation != nil
	if !record.OperationOwned && !recentContains(plan.runtime, change.id) {
		p.actions[change.id] = nil
	} else {
		p.actions[change.id] = record
	}
	return nil
}

func (s *Scheduler) prepareRunningMembership(ctx chasm.Context, p *projectionMutation, record, old *pb.PublicActionRecord, id string, wasRunning, isRunning bool, batch *queueindex.CountBatch) error {
	count, err := changedRecordCount(p.control.RunningCount, wasRunning, isRunning)
	if err != nil {
		return err
	}
	p.control.RunningCount = count
	key := runningRecordKey(record.Sequence)
	existing, present := s.RunningRecords.Get(ctx, key)
	if present != wasRunning {
		return serviceerror.NewInternal("running public action membership mismatch")
	}
	if present && (existing.Sequence != record.Sequence || existing.OperationId != id || !proto.Equal(existing.Execution, old.Action.Result.StartWorkflowResult)) {
		return serviceerror.NewInternal("running public action identity mismatch")
	}
	if isRunning {
		if err := batch.InsertSequence(runningProjectionIndex, record.Sequence, queueindex.CountKey{}); err != nil {
			return err
		}
		p.running[key] = &pb.RunningPublicAction{Sequence: record.Sequence, OperationId: id, Execution: common.CloneProto(record.Action.Result.StartWorkflowResult)}
	} else {
		if err := batch.DeleteSequence(runningProjectionIndex, record.Sequence); err != nil {
			return err
		}
		p.running[key] = nil
	}
	return nil
}

func (s *Scheduler) publishActionRecords(ctx chasm.MutableContext, p *projectionMutation) {
	for _, change := range p.indexes {
		if change.Delete {
			s.ProjectionPages.Delete(ctx, change.Key)
		} else {
			s.ProjectionPages.Put(ctx, change.Key, &commonpb.Payload{Data: change.Value})
		}
	}
	for id, record := range p.actions {
		if record == nil {
			s.ActionRecords.Delete(ctx, id)
		} else {
			s.ActionRecords.Put(ctx, id, record)
		}
	}
	for key, record := range p.running {
		if record == nil {
			s.RunningRecords.Delete(ctx, key)
		} else {
			s.RunningRecords.Put(ctx, key, record)
		}
	}
	s.ProjectionStore = p.control
}

func (s *Scheduler) runningProjection(ctx chasm.Context) ([]*commonpb.WorkflowExecution, error) {
	if err := s.validatePublicProjection(ctx); err != nil {
		return nil, err
	}
	batch := queueindex.NewCountStoreWithBackend(&indexBackend{ctx: ctx, pages: s.ProjectionPages}).Begin()
	defer batch.Abort()
	var result []*commonpb.WorkflowExecution
	for lower := uint64(1); ; {
		entry, ok, err := batch.SequenceAtOrAfter(runningProjectionIndex, lower)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if entry.Sequence == 0 || entry.Sequence >= s.ProjectionStore.NextActionSequence || entry.Time != (queueindex.CountKey{}) {
			return nil, serviceerror.NewInternal("running public action index is invalid")
		}
		record, ok := s.RunningRecords.Get(ctx, runningRecordKey(entry.Sequence))
		if !ok || record.Sequence != entry.Sequence || record.OperationId == "" || record.Execution.GetWorkflowId() == "" || record.Execution.GetRunId() == "" {
			return nil, serviceerror.NewInternal("running public action is missing or invalid")
		}
		result = append(result, common.CloneProto(record.Execution))
		if int64(len(result)) > s.ProjectionStore.RunningCount {
			return nil, serviceerror.NewInternal("running projection exceeds count")
		}
		if entry.Sequence == math.MaxUint64 {
			break
		}
		lower = entry.Sequence + 1
	}
	if int64(len(result)) != s.ProjectionStore.RunningCount {
		return nil, serviceerror.NewInternal("running projection count mismatch")
	}
	return result, nil
}

func newProjectionMutation(control *pb.ProjectionStoreControl) *projectionMutation {
	return &projectionMutation{control: common.CloneProto(control), actions: make(map[string]*pb.PublicActionRecord), running: make(map[string]*pb.RunningPublicAction)}
}

func (s *Scheduler) prepareIndexedPublicTransition(ctx chasm.Context, before, next decision.State, event decision.Event, admitted bool) (publicTransitionPlan, error) {
	if err := s.validatePublicProjection(ctx); err != nil {
		return publicTransitionPlan{}, err
	}
	runtime := &pb.PublicRuntime{}
	if old, ok := s.PublicRuntime.TryGet(ctx); ok {
		runtime = common.CloneProto(old)
	}
	plan := publicTransitionPlan{runtime: runtime, projection: newProjectionMutation(s.ProjectionStore)}
	skipped, missed, dropped := publicDispositionCounts(before, next, event)
	runtime.OverlapSkipped += skipped
	runtime.MissedCatchupWindow += missed
	if dropped != 0 {
		plan.info = common.CloneProto(s.PublicInfo.Get(ctx))
		plan.info.BufferDropped += dropped
	}
	plan.eviction = retainPublicCompletions(runtime, before, next, event)
	if next.Operation == nil {
		return plan, nil
	}
	id := next.Operation.ID
	record, exists := s.ActionRecords.Get(ctx, id)
	record = common.CloneProto(record)
	if admitted {
		if exists || plan.projection.control.NextActionSequence == math.MaxUint64 {
			return publicTransitionPlan{}, serviceerror.NewInternal("public action admission identity exhausted or reused")
		}
		record = &pb.PublicActionRecord{Sequence: plan.projection.control.NextActionSequence, Action: admittedPublicAction(before, next, event), OperationOwned: true}
		plan.projection.control.NextActionSequence++
	} else if !exists {
		return publicTransitionPlan{}, serviceerror.NewInternal("public action record is missing")
	}
	if err := validatePublicActionRecord(record, id, plan.projection.control.NextActionSequence); err != nil {
		return publicTransitionPlan{}, err
	}
	updatePublicResult(&pb.PublicRuntime{Actions: []*pb.PublicAction{record.Action}}, before, next, event)
	plan.projection.recentVictim = updateRecentActions(runtime, record)
	plan.projection.actions[id] = record
	return plan, nil
}

func validatePublicActionRecord(record *pb.PublicActionRecord, id string, next uint64) error {
	if record == nil || record.Sequence == 0 || record.Sequence >= next || record.Action == nil || record.Action.OperationId != id || record.Action.Result == nil || record.Action.Result.StartWorkflowResult == nil {
		return serviceerror.NewInternal("public action identity mismatch")
	}
	result := record.Action.Result
	if result.StartWorkflowResult.RunId != "" && (result.ActualTime == nil || result.ActualTime.CheckValid() != nil) {
		return serviceerror.NewInternal("public action observation time is invalid")
	}
	return nil
}
