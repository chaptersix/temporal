package scheduler2

import (
	"bytes"
	"encoding/binary"
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
)

func newQueueState() *pb.QueueState {
	return &pb.QueueState{FormatVersion: 1, Generation: 1, Version: 1, NextSequence: 1}
}
func queueCounts(q *pb.QueueState) [6]int64 {
	return [6]int64{q.AutomaticOrdinary, q.AutomaticCancel, q.AutomaticTerminate, q.ExplicitOrdinary, q.ExplicitCancel, q.ExplicitTerminate}
}
func setQueueCounts(q *pb.QueueState, c [6]int64) {
	q.AutomaticOrdinary, q.AutomaticCancel, q.AutomaticTerminate, q.ExplicitOrdinary, q.ExplicitCancel, q.ExplicitTerminate = c[0], c[1], c[2], c[3], c[4], c[5]
}
func queueTotal(c [6]int64) (int64, error) {
	var total int64
	for _, n := range c {
		if n < 0 || n > math.MaxInt64-total {
			return 0, serviceerror.NewInternal("invalid queue count")
		}
		total += n
	}
	return total, nil
}
func (s *Scheduler) queueSummary() (decision.QueueSummary, error) {
	q := s.IndexedQueue
	if q == nil {
		return decision.QueueSummary{}, nil
	}
	if q.FormatVersion != 1 || q.Generation == 0 || q.Version == 0 || q.NextSequence == 0 || len(s.Queue) != 0 {
		return decision.QueueSummary{}, serviceerror.NewInternal("invalid indexed queue format")
	}
	c := queueCounts(q)
	if _, err := queueTotal(c); err != nil {
		return decision.QueueSummary{}, err
	}
	if err := validateCompletedQueueDrain(q); err != nil {
		return decision.QueueSummary{}, err
	}
	return decision.QueueSummary{Enabled: true, Generation: q.Generation, Version: q.Version, Counts: c, Maintenance: q.CompletedDrain != nil}, nil
}
func validateCompletedQueueDrain(q *pb.QueueState) error {
	if d := q.CompletedDrain; d != nil {
		if d.SourceGeneration == 0 || d.SourceGeneration >= q.Generation || d.Remaining <= 0 || d.MaximumSequence == 0 || d.MaximumSequence >= q.NextSequence || d.NextSequence == 0 || d.NextSequence > d.MaximumSequence || d.NextClass >= 6 || len(d.Threshold) != 12 || d.FirstClass >= 6 || d.SelectedClass >= 6 || d.SelectedSequence > d.MaximumSequence || d.FirstSequence > d.SelectedSequence || d.Removed <= 0 || d.Missed < 0 || d.Missed > d.Removed || d.Superseded < 0 || d.Superseded > d.Removed-d.Missed {
			return serviceerror.NewInternal("invalid completed queue drain")
		}
	}
	return nil
}
func queueIndex(generation uint64, class int) queueindex.SequenceIndex {
	var id queueindex.SequenceIndex
	binary.BigEndian.PutUint64(id[:8], generation)
	id[8] = byte(class)
	return id
}
func queueRecordKey(sequence uint64) string { return fmt.Sprintf("%016x", sequence) }
func queueTime(q decision.Queued) queueindex.CountKey {
	full := queueindex.NewTimeKey(q.ActualTime.Time(q.Actual), 0)
	return queueindex.CountKey(full[:12])
}
func queueThreshold(now, catchup int64) queueindex.CountKey {
	full := queueindex.NewTimeKey(time.Unix(0, now).Add(-time.Duration(catchup)), 0)
	return queueindex.CountKey(full[:12])
}
func decodeQueued(q *pb.QueuedOccurrence) decision.Queued {
	return decision.Queued{Nominal: q.Nominal, Actual: q.Actual, NominalTime: decodeDecisionTime(q.NominalTime), ActualTime: decodeDecisionTime(q.ActualTime), WorkflowID: q.WorkflowId, Policy: q.Policy, ManualID: q.ManualId, BackfillID: q.BackfillId}
}
func encodeQueued(q decision.Queued) *pb.QueuedOccurrence {
	return &pb.QueuedOccurrence{Nominal: q.Nominal, Actual: q.Actual, NominalTime: encodeDecisionTime(q.NominalTime), ActualTime: encodeDecisionTime(q.ActualTime), WorkflowId: q.WorkflowID, Policy: q.Policy, ManualId: q.ManualID, BackfillId: q.BackfillID}
}
func (s *Scheduler) queueBatch(ctx chasm.Context) *queueindex.CountBatch {
	return queueindex.NewCountStoreWithBackend(s.queueIndexReader(ctx)).Begin()
}

// queuePlanBatch models a prospective append only in an unpublished index overlay.
func (s *Scheduler) queuePlanBatch(ctx chasm.Context, state decision.State) (*queueindex.CountBatch, error) {
	summary, err := s.queueSummary()
	if err != nil {
		return nil, err
	}
	if !summary.Enabled {
		return nil, serviceerror.NewInternal("queue preview requires indexed storage")
	}
	b := s.queueBatch(ctx)
	if summary == state.IndexedQueue {
		return b, nil
	}
	change := state.QueueChange
	if !change.Appended || change.Drained || summary.Version == math.MaxUint64 || s.IndexedQueue.NextSequence == math.MaxUint64 {
		b.Abort()
		return nil, serviceerror.NewInternal("queue preview fence mismatch")
	}
	summary.Counts[decision.QueueClass(change.Append)]++
	summary.Version++
	if summary != state.IndexedQueue {
		b.Abort()
		return nil, serviceerror.NewInternal("queue preview summary mismatch")
	}
	if err = b.InsertSequence(queueIndex(summary.Generation, decision.QueueClass(change.Append)), s.IndexedQueue.NextSequence, queueTime(change.Append)); err != nil {
		b.Abort()
		return nil, err
	}
	return b, nil
}

// planQueueDrain reads bounded index paths and at most one selected record.
// A nil plan means blocked, empty or maintenance-deferred, never disposition.
func (s *Scheduler) planQueueDrain(ctx chasm.Context, state decision.State, now, catchup int64) (*decision.QueueDrainPlan, error) {
	if err := decision.ValidateQueueSummary(state); err != nil {
		return nil, err
	}
	if catchup < 0 {
		return nil, serviceerror.NewInvalidArgument("catchup window must be nonnegative")
	}
	if !state.IndexedQueue.Enabled {
		return nil, serviceerror.NewInternal("queue planner requires indexed state")
	}
	if state.IndexedQueue.Maintenance || state.Root.BlockingOperationID != "" || decision.QueueLength(state) == 0 {
		return nil, nil
	}
	b, err := s.queuePlanBatch(ctx, state)
	if err != nil {
		return nil, err
	}
	defer b.Abort()
	p := &decision.QueueDrainPlan{Fence: decision.QueueDrainFence{Summary: state.IndexedQueue, Root: state.Root, Capacity: state.Capacity, Now: now, Catchup: catchup}}
	automatic := !state.Root.Paused && (!state.Root.Limited || state.Root.Remaining > 0)
	threshold := queueThreshold(now, catchup)
	candidates, err := queryQueueCandidates(b, state, threshold, automatic)
	if err != nil {
		return nil, err
	}
	eligible, first, last := candidates.eligible, candidates.first, candidates.last
	firstClass, selectedClass := candidates.firstClass, candidates.lastClass
	for class, count := range state.IndexedQueue.Counts {
		p.Ineligible[class] = count - eligible[class]
	}
	if first.Sequence == 0 {
		return validatedQueuePlan(state, p)
	}
	if last.Sequence == 0 {
		last = first
		selectedClass = firstClass
	}
	p.Selected = true
	p.FirstSequence = first.Sequence
	p.FirstClass = firstClass
	p.SelectedSequence = last.Sequence
	if state.QueueChange.Appended && last.Sequence == s.IndexedQueue.NextSequence {
		p.Candidate = state.QueueChange.Append
	} else {
		record, ok, err := s.queuedRecord(ctx, last.Sequence)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, serviceerror.NewInternal("selected queue record absent")
		}
		p.Candidate = decodeQueued(record)
	}
	if decision.QueueClass(p.Candidate) != selectedClass || queueTime(p.Candidate) != last.Time {
		return nil, serviceerror.NewInternal("selected queue record mismatch")
	}
	if err = fillQueueDispositions(b, state, threshold, eligible, p); err != nil {
		return nil, err
	}
	return validatedQueuePlan(state, p)
}

type queueCandidates struct {
	eligible              [6]int64
	first, last           queueindex.SequenceEntry
	firstClass, lastClass int
}

func queryQueueClass(b *queueindex.CountBatch, index queueindex.SequenceIndex, lower queueindex.CountKey, count int64, effect bool) (eligible int64, first, last queueindex.SequenceEntry, err error) {
	n, err := b.SequenceCount(index, 0, math.MaxUint64, lower)
	if err != nil {
		return 0, first, last, err
	}
	if n > uint64(count) {
		return 0, first, last, serviceerror.NewInternal("queue class count mismatch")
	}
	if n == 0 {
		return 0, first, last, nil
	}
	first, ok, err := b.SequenceExtreme(index, lower, false)
	if err != nil {
		return 0, first, last, err
	}
	if !ok {
		return 0, first, last, serviceerror.NewInternal("queue eligible extreme absent")
	}
	if effect {
		last, ok, err = b.SequenceExtreme(index, lower, true)
		if err != nil {
			return 0, first, last, err
		}
		if !ok {
			return 0, first, last, serviceerror.NewInternal("queue effect extreme absent")
		}
	}
	return int64(n), first, last, nil
}
func queryQueueCandidates(b *queueindex.CountBatch, state decision.State, threshold queueindex.CountKey, automatic bool) (queueCandidates, error) {
	var result queueCandidates
	for class, count := range state.IndexedQueue.Counts {
		if class < 3 && !automatic {
			continue
		}
		lower := queueindex.CountKey{}
		if class < 3 {
			lower = threshold
		}
		n, first, last, err := queryQueueClass(b, queueIndex(state.IndexedQueue.Generation, class), lower, count, class%3 != 0)
		if err != nil {
			return result, err
		}
		result.eligible[class] = n
		if n == 0 {
			continue
		}
		if result.first.Sequence == 0 || first.Sequence < result.first.Sequence {
			result.first = first
			result.firstClass = class
		}
		if last.Sequence > result.last.Sequence {
			result.last = last
			result.lastClass = class
		}
	}
	return result, nil
}
func fillQueueDispositions(b *queueindex.CountBatch, state decision.State, threshold queueindex.CountKey, eligible [6]int64, p *decision.QueueDrainPlan) error {
	firstClass, selectedClass := p.FirstClass, decision.QueueClass(p.Candidate)
	firstSequence, lastSequence := p.FirstSequence, p.SelectedSequence
	for class, n := range eligible {
		replaced := int64(0)
		lower := queueindex.CountKey{}
		if class < 3 {
			lower = threshold
		}
		if class%3 != 0 && n > 0 {
			count, err := b.SequenceCount(queueIndex(state.IndexedQueue.Generation, class), 0, lastSequence-1, lower)
			if err != nil {
				return err
			}
			if count > uint64(n) {
				return serviceerror.NewInternal("queue supersession count mismatch")
			}
			replaced = int64(count)
		} else if class == firstClass && firstSequence < lastSequence {
			replaced = 1
		}
		p.Superseded[class] = replaced
		p.Retained[class] = n - replaced
		if class == selectedClass {
			p.Retained[class]--
		}
	}
	return nil
}

func validatedQueuePlan(state decision.State, p *decision.QueueDrainPlan) (*decision.QueueDrainPlan, error) {
	if err := decision.ValidateQueueDrainPlan(state, decision.Event{Now: p.Fence.Now, CatchupWindow: p.Fence.Catchup}, *p); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Scheduler) stageQueueAppend(ctx chasm.MutableContext, q decision.Queued) error {
	summary, err := s.queueSummary()
	if err != nil {
		return err
	}
	if !summary.Enabled || q.WorkflowID == "" || !decision.ValidPolicy(q.Policy) {
		return serviceerror.NewInternal("invalid indexed queue append")
	}
	total, err := queueTotal(summary.Counts)
	if err != nil {
		return err
	}
	state := s.IndexedQueue
	if state.NextSequence == math.MaxUint64 || state.Version == math.MaxUint64 || total == math.MaxInt64 {
		return serviceerror.NewFailedPrecondition("queue identity or count exhausted")
	}
	key := queueRecordKey(state.NextSequence)
	if _, ok, err := s.queuedRecord(ctx, state.NextSequence); err != nil {
		return err
	} else if ok {
		return serviceerror.NewInternal("queue record identity collision")
	}
	next := common.CloneProto(state)
	counts := summary.Counts
	counts[decision.QueueClass(q)]++
	setQueueCounts(next, counts)
	next.NextSequence++
	next.Version++
	b, capture := s.queueMutationBatch(ctx)
	defer b.Abort()
	if err = b.InsertSequence(queueIndex(state.Generation, decision.QueueClass(q)), state.NextSequence, queueTime(q)); err != nil {
		return err
	}
	if err = b.Commit(); err != nil {
		return err
	}
	if err = s.stagePackedQueueMutation(ctx, capture, true, state.NextSequence, encodeQueued(q)); err != nil {
		return err
	}
	if s.PackedStorage == nil {
		s.QueueRecords.Put(ctx, key, encodeQueued(q))
	}
	s.IndexedQueue = next
	return nil
}

func minimumQueueRecord(b *queueindex.CountBatch, generation uint64) (queueindex.SequenceEntry, int, error) {
	var minimum queueindex.SequenceEntry
	classFound := 0
	for class := 0; class < 6; class++ {
		entry, ok, err := b.SequenceExtreme(queueIndex(generation, class), queueindex.CountKey{}, false)
		if err != nil {
			return minimum, 0, err
		}
		if ok && (minimum.Sequence == 0 || entry.Sequence < minimum.Sequence) {
			minimum = entry
			classFound = class
		}
	}
	return minimum, classFound, nil
}
func (s *Scheduler) stageQueueDrain(ctx chasm.MutableContext, p decision.QueueDrainPlan, state decision.State) error {
	summary, err := s.queueSummary()
	if err != nil {
		return err
	}
	if summary != state.IndexedQueue {
		return serviceerror.NewInternal("queue drain publication fence mismatch")
	}
	if err = decision.ValidateQueueDrainPlan(state, decision.Event{Now: p.Fence.Now, CatchupWindow: p.Fence.Catchup}, p); err != nil {
		return err
	}
	if p.Selected && !decision.OperationAvailable(state) {
		return serviceerror.NewInternal("queue drain lacks operation capacity")
	}
	q := s.IndexedQueue
	if q.Generation == math.MaxUint64 || q.Version == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("queue generation exhausted")
	}
	total, err := queueTotal(summary.Counts)
	if err != nil {
		return err
	}
	if total == 0 {
		return serviceerror.NewInternal("empty queue drain")
	}
	b := s.queueBatch(ctx)
	defer b.Abort()
	if err = validateDrainGenerations(b, q.Generation, summary.Counts); err != nil {
		return err
	}
	first, class, err := minimumQueueRecord(b, q.Generation)
	if err != nil {
		return err
	}
	if first.Sequence == 0 {
		return serviceerror.NewInternal("queue source cursor absent")
	}
	threshold := queueThreshold(p.Fence.Now, p.Fence.Catchup)
	d := &pb.CompletedDrain{SourceGeneration: q.Generation, MaximumSequence: q.NextSequence - 1, Remaining: total, NextSequence: first.Sequence, NextClass: uint32(class), AutomaticEnabled: !state.Root.Paused && (!state.Root.Limited || state.Root.Remaining > 0), Threshold: slices.Clone(threshold[:]), FirstSequence: p.FirstSequence, FirstClass: uint32(p.FirstClass), SelectedSequence: p.SelectedSequence}
	if p.Selected {
		d.SelectedClass = uint32(decision.QueueClass(p.Candidate))
	}
	retained, err := queueTotal(p.Retained)
	if err != nil {
		return err
	}
	d.Removed = total - retained
	for class := 0; class < 6; class++ {
		d.Superseded += p.Superseded[class]
		if class < 3 && d.AutomaticEnabled {
			d.Missed += p.Ineligible[class]
		}
	}
	next := common.CloneProto(q)
	next.Generation++
	next.Version++
	next.CompletedDrain = d
	setQueueCounts(next, p.Retained)
	s.IndexedQueue = next
	return nil
}

func validateDrainGenerations(b *queueindex.CountBatch, generation uint64, counts [6]int64) error {
	for class, count := range counts {
		n, err := b.SequenceCount(queueIndex(generation, class), 0, math.MaxUint64, queueindex.CountKey{})
		if err != nil {
			return err
		}
		if n != uint64(count) {
			return serviceerror.NewInternal("queue physical count mismatch")
		}
		n, err = b.SequenceCount(queueIndex(generation+1, class), 0, math.MaxUint64, queueindex.CountKey{})
		if err != nil {
			return err
		}
		if n != 0 {
			return serviceerror.NewInternal("queue next generation collision")
		}
	}
	return nil
}

func survivesQueueDrain(d *pb.CompletedDrain, sequence uint64, class int, at queueindex.CountKey) bool {
	if d.SelectedSequence == 0 {
		return false
	}
	if class < 3 && (!d.AutomaticEnabled || bytes.Compare(at[:], d.Threshold) < 0) {
		return false
	}
	if sequence == d.SelectedSequence {
		return false
	}
	return sequence > d.SelectedSequence || sequence != d.FirstSequence && class%3 == 0
}

// maintainQueue performs one physical source record move/removal. Logical counts
// were already updated at drain publication and must not be adjusted here.
func (s *Scheduler) maintainQueue(ctx chasm.MutableContext) (bool, error) {
	summary, err := s.queueSummary()
	if err != nil {
		return false, err
	}
	if !summary.Enabled || !summary.Maintenance {
		return false, nil
	}
	q := s.IndexedQueue
	d := q.CompletedDrain
	sequence, class := d.NextSequence, int(d.NextClass)
	record, ok, err := s.queuedRecord(ctx, sequence)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, serviceerror.NewInternal("queue cleanup record absent")
	}
	value := decodeQueued(record)
	if decision.QueueClass(value) != class {
		return false, serviceerror.NewInternal("queue cleanup class mismatch")
	}
	at := queueTime(value)
	b, capture := s.queueMutationBatch(ctx)
	defer b.Abort()
	entry, ok, err := b.Sequence(queueIndex(d.SourceGeneration, class), sequence)
	if err != nil {
		return false, err
	}
	if !ok || entry.Time != at {
		return false, serviceerror.NewInternal("queue cleanup source mismatch")
	}
	keep := survivesQueueDrain(d, sequence, class, at)
	if keep {
		if err = b.InsertSequence(queueIndex(q.Generation, class), sequence, at); err != nil {
			return false, err
		}
	}
	if err = b.DeleteSequence(queueIndex(d.SourceGeneration, class), sequence); err != nil {
		return false, err
	}
	next := common.CloneProto(q)
	next.CompletedDrain.Remaining--
	following, nextClass, err := minimumQueueRecord(b, d.SourceGeneration)
	if err != nil {
		return false, err
	}
	if next.CompletedDrain.Remaining == 0 {
		if following.Sequence != 0 {
			return false, serviceerror.NewInternal("queue cleanup remaining count mismatch")
		}
		next.CompletedDrain = nil
	} else {
		if following.Sequence <= sequence || following.Sequence > d.MaximumSequence {
			return false, serviceerror.NewInternal("queue cleanup cursor mismatch")
		}
		next.CompletedDrain.NextSequence = following.Sequence
		next.CompletedDrain.NextClass = uint32(nextClass)
	}
	if err = b.Commit(); err != nil {
		return false, err
	}
	if err = s.stagePackedQueueMutation(ctx, capture, !keep, sequence, nil); err != nil {
		return false, err
	}
	if !keep && s.PackedStorage == nil {
		s.QueueRecords.Delete(ctx, queueRecordKey(sequence))
	}
	s.IndexedQueue = next
	return true, nil
}

// queueProjection is explicit full-state inspection, never a hot consumer.
// It includes physical cleanup debt even when the logical result is empty.
func (s *Scheduler) queueProjection(ctx chasm.Context) ([]decision.Queued, error) {
	summary, err := s.queueSummary()
	if err != nil {
		return nil, err
	}
	if !summary.Enabled {
		var result []decision.Queued
		for _, q := range s.Queue {
			result = append(result, decodeQueued(q))
		}
		return result, nil
	}
	b := s.queueBatch(ctx)
	defer b.Abort()
	// Export walks indexed live sequences by removing them from an unpublished
	// overlay. Abort discards all temporary page/refcount changes.
	generations := []uint64{s.IndexedQueue.Generation}
	if d := s.IndexedQueue.CompletedDrain; d != nil {
		generations = append(generations, d.SourceGeneration)
	}
	var items []queueProjectionItem
	for _, generation := range generations {
		records, err := s.projectQueueGeneration(ctx, b, generation)
		if err != nil {
			return nil, err
		}
		items = append(items, records...)
	}
	slices.SortFunc(items, func(a, b queueProjectionItem) int {
		if a.sequence < b.sequence {
			return -1
		}
		if a.sequence > b.sequence {
			return 1
		}
		return 0
	})
	var result []decision.Queued
	var counts [6]int64
	for _, item := range items {
		result = append(result, item.value)
		counts[decision.QueueClass(item.value)]++
	}
	if counts != summary.Counts {
		return nil, serviceerror.NewInternal("queue export logical count mismatch")
	}
	return result, nil
}

type queueProjectionItem struct {
	sequence uint64
	value    decision.Queued
}

func (s *Scheduler) projectQueueGeneration(ctx chasm.Context, b *queueindex.CountBatch, generation uint64) ([]queueProjectionItem, error) {
	var items []queueProjectionItem
	for {
		e, class, err := minimumQueueRecord(b, generation)
		if err != nil {
			return nil, err
		}
		if e.Sequence == 0 {
			break
		}
		record, ok, err := s.queuedRecord(ctx, e.Sequence)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, serviceerror.NewInternal("queue export record absent")
		}
		q := decodeQueued(record)
		if decision.QueueClass(q) != class || queueTime(q) != e.Time {
			return nil, serviceerror.NewInternal("queue export record mismatch")
		}
		d := s.IndexedQueue.CompletedDrain
		if d == nil || generation != d.SourceGeneration || survivesQueueDrain(d, e.Sequence, class, e.Time) {
			items = append(items, queueProjectionItem{e.Sequence, q})
		}
		if err = b.DeleteSequence(queueIndex(generation, class), e.Sequence); err != nil {
			return nil, err
		}
	}
	return items, nil
}
