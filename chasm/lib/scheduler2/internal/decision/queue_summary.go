package decision

import (
	"math"

	"go.temporal.io/api/serviceerror"
)

// QueueSummary is the fixed control view of an indexed queue. Counts are
// automatic ordinary/cancel/terminate followed by explicit ordinary/cancel/terminate.
// Maintenance denotes one completed drain awaiting physical cleanup.
type QueueSummary struct {
	Enabled             bool
	Generation, Version uint64
	Counts              [6]int64
	Maintenance         bool
}

// QueueDrainFence binds an internal query result to its exact control snapshot.
type QueueDrainFence struct {
	Summary      QueueSummary
	Root         Root
	Capacity     Capacity
	Now, Catchup int64
}

// QueueDrainPlan is supplied by the keyed storage adapter, never by an RPC.
// The reducer checks consistency; storage membership and extrema are established
// by the adapter's bounded index queries in the same enclosing transaction.
type QueueDrainPlan struct {
	Fence                            QueueDrainFence
	Selected                         bool
	Candidate                        Queued
	FirstSequence, SelectedSequence  uint64
	FirstClass                       int
	Retained, Ineligible, Superseded [6]int64
}

// QueueChange is an ephemeral publication result. Step clears it for every event;
// auxiliary control transformations retain it until the caller publishes once.
type QueueChange struct {
	Appended bool
	Append   Queued
	Drained  bool
	Drain    QueueDrainPlan
}

// QueueClass preserves concrete policy while partitioning queue queries.
func QueueClass(q Queued) int {
	class := int(effectPolicy(q.Policy))
	if q.ManualID != 0 || q.BackfillID != 0 {
		class += 3
	}
	return class
}

// QueueLength uses persisted logical membership for the indexed representation.
func QueueLength(state State) int64 {
	if !state.IndexedQueue.Enabled {
		return int64(len(state.Queue))
	}
	var count int64
	for _, n := range state.IndexedQueue.Counts {
		count += n
	}
	return count
}

// QueueEffect considers all logically retained effect entries, even ineligible ones.
func QueueEffect(state State) EffectKind {
	if state.IndexedQueue.Enabled {
		if state.IndexedQueue.Counts[2] != 0 || state.IndexedQueue.Counts[5] != 0 {
			return Terminate
		}
		if state.IndexedQueue.Counts[1] != 0 || state.IndexedQueue.Counts[4] != 0 {
			return Cancel
		}
		return NoEffect
	}
	desired := NoEffect
	for _, q := range state.Queue {
		desired = max(desired, effectPolicy(q.Policy))
	}
	return desired
}

// ValidateQueueSummary checks the persisted control view before any scalar reads.
func ValidateQueueSummary(state State) error {
	if !state.IndexedQueue.Enabled {
		return nil
	}
	if len(state.Queue) != 0 || state.IndexedQueue.Generation == 0 || state.IndexedQueue.Version == 0 {
		return serviceerror.NewInternal("invalid indexed queue representation")
	}
	var total int64
	for _, count := range state.IndexedQueue.Counts {
		if count < 0 || count > math.MaxInt64-total {
			return serviceerror.NewInternal("invalid indexed queue count")
		}
		total += count
	}
	if state.Capacity.BufferedOperations < 0 {
		return serviceerror.NewInternal("negative buffered operation count")
	}
	return nil
}

func appendQueued(next State, candidate Queued) (State, error) {
	if !next.IndexedQueue.Enabled {
		next.Queue = append(next.Queue, candidate)
		return next, nil
	}
	if next.IndexedQueue.Version == math.MaxUint64 || QueueLength(next) == math.MaxInt64 {
		return State{}, serviceerror.NewFailedPrecondition("indexed queue identity or count exhausted")
	}
	next.IndexedQueue.Counts[QueueClass(candidate)]++
	next.IndexedQueue.Version++
	next.QueueChange = QueueChange{Appended: true, Append: candidate}
	return next, nil
}

func drainIndexed(next State, event Event) (State, error) {
	if next.IndexedQueue.Maintenance {
		return next, nil
	}
	if event.QueueDrain == nil {
		return State{}, serviceerror.NewInternal("indexed drain requires a query plan")
	}
	plan := *event.QueueDrain
	if err := ValidateQueueDrainPlan(next, event, plan); err != nil {
		return State{}, err
	}
	if plan.Selected && !OperationAvailable(next) {
		return next, nil
	}
	if plan.Selected && len(event.Request) == 0 {
		return State{}, serviceerror.NewInvalidArgument("drain admission requires current request")
	}
	if next.IndexedQueue.Version == math.MaxUint64 || next.IndexedQueue.Generation == math.MaxUint64 {
		return State{}, serviceerror.NewFailedPrecondition("indexed queue identity exhausted")
	}
	var superseded int64
	for _, n := range plan.Superseded {
		superseded += n
	}
	if next.Root.OverlapSuperseded > math.MaxInt64-superseded {
		return State{}, serviceerror.NewFailedPrecondition("overlap counter exhausted")
	}
	next.Root.OverlapSuperseded += superseded
	next.IndexedQueue.Counts = plan.Retained
	next.IndexedQueue.Generation++
	next.IndexedQueue.Version++
	next.IndexedQueue.Maintenance = true
	next.QueueChange = QueueChange{Drained: true, Drain: plan}
	if plan.Selected {
		return startOperation(next, event, plan.Candidate)
	}
	return next, nil
}

// ValidateQueueDrainPlan checks the fixed query result before publication.
func ValidateQueueDrainPlan(state State, event Event, plan QueueDrainPlan) error {
	if err := ValidateQueueSummary(state); err != nil {
		return err
	}
	if !state.IndexedQueue.Enabled || state.IndexedQueue.Maintenance || QueueLength(state) == 0 || occupied(state) || event.CatchupWindow < 0 {
		return serviceerror.NewInternal("indexed drain is not enabled")
	}
	expected := QueueDrainFence{Summary: state.IndexedQueue, Root: state.Root, Capacity: state.Capacity, Now: event.Now, Catchup: event.CatchupWindow}
	if plan.Fence != expected {
		return serviceerror.NewInternal("indexed drain control fence mismatch")
	}
	if err := validateDrainSelection(state, event, plan); err != nil {
		return err
	}
	automatic := !state.Root.Paused && (!state.Root.Limited || state.Root.Remaining > 0)
	for class, count := range state.IndexedQueue.Counts {
		if err := validateDrainClass(plan, class, count, automatic); err != nil {
			return err
		}
	}
	if plan.Selected && state.IndexedQueue.Counts[plan.FirstClass]-plan.Ineligible[plan.FirstClass] == 0 {
		return serviceerror.NewInternal("indexed drain first class has no eligible member")
	}
	return nil
}

func validateDrainSelection(state State, event Event, plan QueueDrainPlan) error {
	if plan.Selected {
		if plan.FirstSequence == 0 || plan.SelectedSequence < plan.FirstSequence || plan.FirstClass < 0 || plan.FirstClass >= 6 || plan.Candidate.WorkflowID == "" || !ValidPolicy(plan.Candidate.Policy) || !queueEligible(state, plan.Candidate, event.Now, event.CatchupWindow) {
			return serviceerror.NewInternal("invalid indexed drain selection")
		}

		if (plan.FirstSequence == plan.SelectedSequence && plan.FirstClass != QueueClass(plan.Candidate)) || (effectPolicy(plan.Candidate.Policy) == NoEffect && plan.FirstSequence != plan.SelectedSequence) {
			return serviceerror.NewInternal("indexed drain selection order is inconsistent")
		}
	} else if plan.Candidate != (Queued{}) || plan.FirstSequence != 0 || plan.SelectedSequence != 0 || plan.FirstClass != 0 {
		return serviceerror.NewInternal("unexpected indexed drain selection")
	}
	return nil
}
func validateDrainClass(plan QueueDrainPlan, class int, count int64, automatic bool) error {
	ineligible, retained, superseded := plan.Ineligible[class], plan.Retained[class], plan.Superseded[class]
	if ineligible < 0 || ineligible > count || retained < 0 || retained > count-ineligible || superseded < 0 || superseded > count-ineligible-retained {
		return serviceerror.NewInternal("invalid indexed drain disposition counts")
	}
	if (class >= 3 && ineligible != 0) || (class < 3 && !automatic && ineligible != count) {
		return serviceerror.NewInternal("invalid indexed drain eligibility counts")
	}
	selected := int64(0)
	if plan.Selected && QueueClass(plan.Candidate) == class {
		selected = 1
	}
	if count-ineligible-retained-superseded != selected {
		return serviceerror.NewInternal("indexed drain counts do not conserve membership")
	}
	if !plan.Selected && (retained != 0 || superseded != 0) {
		return serviceerror.NewInternal("empty indexed drain retains eligible membership")
	}
	if class%3 == 0 {
		expectedSuperseded := int64(0)
		if plan.Selected && plan.FirstSequence < plan.SelectedSequence && plan.FirstClass == class {
			expectedSuperseded = 1
		}
		if superseded != expectedSuperseded {
			return serviceerror.NewInternal("invalid ordinary predecessor disposition")
		}
	}
	return nil
}
