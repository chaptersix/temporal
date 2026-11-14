package decision

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// Queued contains identity metadata, never an admitted request snapshot.
type Queued struct {
	NominalTime Timestamp
	ActualTime  Timestamp
	BackfillID  uint64
	ManualID    uint64
	Nominal     int64
	Actual      int64
	WorkflowID  string
	Policy      enumspb.ScheduleOverlapPolicy
}

func ValidPolicy(p enumspb.ScheduleOverlapPolicy) bool {
	return effectPolicy(p) != NoEffect || p == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || p == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED || p == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP || p == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || p == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
}

func occupied(s State) bool { return s.Root.BlockingOperationID != "" }

func enqueue(next State, event Event) (State, error) {
	policy := next.Root.Policy
	if !ValidPolicy(policy) {
		return State{}, serviceerror.NewInvalidArgument("unsupported overlap policy")
	}
	if policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE && policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL && effectPolicy(policy) == NoEffect {
		return next, nil
	}
	if !QueueAvailable(next) || (policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE && QueueLength(next) > 0) {
		next.Root.BufferDropped++
		return next, nil
	}
	if event.WorkflowID == "" || event.Actual < event.Nominal || event.Actual > event.Now {
		return State{}, serviceerror.NewInvalidArgument("buffering requires due occurrence identity")
	}
	return appendQueued(next, Queued{Nominal: event.Nominal, Actual: event.Actual, WorkflowID: event.WorkflowID, Policy: policy})
}

// DrainIndex selects a finite-profile waiter without preparing payloads.
// Indexed callers must use an adapter query plan instead of a slice position.
func DrainIndex(state State, now, catchup int64) int {
	if state.IndexedQueue.Enabled || occupied(state) || catchup < 0 {
		return -1
	}
	selected := -1
	for i, q := range state.Queue {
		if queueEligible(state, q, now, catchup) && (selected < 0 || effectPolicy(q.Policy) != NoEffect) {
			selected = i
		}
	}
	return selected
}

// CanDrain handles finite-profile capacity waits and ineligible disposition.
// Indexed callers use the adapter query plan and its maintenance fence.
func CanDrain(state State, now, catchup int64) bool {
	return !state.IndexedQueue.Enabled && NeedsDrain(state) && (DrainIndex(state, now, catchup) < 0 || OperationAvailable(state))
}

// NeedsDrain distinguishes internal pending work from waiting on an active operation.
func NeedsDrain(state State) bool { return QueueLength(state) > 0 && !occupied(state) }

func drain(next State, event Event) (State, error) {
	if event.CatchupWindow < 0 {
		return State{}, serviceerror.NewInvalidArgument("catchup window must be nonnegative")
	}
	if !NeedsDrain(next) {
		return next, nil
	}
	if next.IndexedQueue.Enabled {
		return drainIndexed(next, event)
	}
	index := DrainIndex(next, event.Now, event.CatchupWindow)
	if index < 0 {
		next.Queue = nil
		return next, nil
	}
	if !OperationAvailable(next) {
		return next, nil
	}
	if len(event.Request) == 0 {
		return State{}, serviceerror.NewInvalidArgument("drain admission requires current request")
	}
	candidate := next.Queue[index]
	var retained []Queued
	firstEligible := true
	for i, q := range next.Queue {
		if !queueEligible(next, q, event.Now, event.CatchupWindow) {
			continue
		}
		replaced := i < index && (firstEligible || effectPolicy(q.Policy) != NoEffect)
		firstEligible = false
		if replaced {
			next.Root.OverlapSuperseded++
		} else if i != index {
			retained = append(retained, q)
		}
	}
	next.Queue = retained
	return startOperation(next, event, candidate)
}

func queueEligible(state State, q Queued, now, catchup int64) bool {
	return q.ManualID != 0 || q.BackfillID != 0 || (!state.Root.Paused && (!state.Root.Limited || state.Root.Remaining > 0) && !q.ActualTime.Time(q.Actual).Before(time.Unix(0, now).Add(-time.Duration(catchup))))
}
