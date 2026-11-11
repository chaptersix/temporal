package decision

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// Queued contains identity metadata, never an admitted request snapshot.
type Queued struct {
	Nominal    int64
	Actual     int64
	WorkflowID string
	Policy     enumspb.ScheduleOverlapPolicy
}

func ValidPolicy(p enumspb.ScheduleOverlapPolicy) bool {
	return p == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || p == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED || p == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP || p == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || p == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
}

func occupied(s State) bool { return s.Root.BlockingOperationID != "" }

func enqueue(next State, event Event) (State, error) {
	policy := next.Root.Policy
	if !ValidPolicy(policy) {
		return State{}, serviceerror.NewInvalidArgument("unsupported overlap policy")
	}
	if policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE && policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL {
		return next, nil
	}
	if len(next.Queue) >= 4 || (policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE && len(next.Queue) > 0) {
		next.Root.BufferDropped++
		return next, nil
	}
	if event.WorkflowID == "" || event.Actual < event.Nominal || event.Actual > event.Now {
		return State{}, serviceerror.NewInvalidArgument("buffering requires due occurrence identity")
	}
	next.Queue = append(next.Queue, Queued{Nominal: event.Nominal, Actual: event.Actual, WorkflowID: event.WorkflowID, Policy: policy})
	return next, nil
}

// DrainIndex chooses the first currently eligible waiter without preparing payloads.
func DrainIndex(state State, now, catchup int64) int {
	if occupied(state) || state.Root.Paused || (state.Root.Limited && state.Root.Remaining <= 0) || catchup < 0 {
		return -1
	}
	for i, q := range state.Queue {
		if now <= q.Actual || now-q.Actual <= catchup {
			return i
		}
	}
	return -1
}

// CanDrain excludes external capacity waits while permitting ineligible disposition.
func CanDrain(state State, now, catchup int64) bool {
	return NeedsDrain(state) && (DrainIndex(state, now, catchup) < 0 || state.Root.Unfinished < 4)
}

// NeedsDrain distinguishes internal pending work from waiting on an active operation.
func NeedsDrain(state State) bool { return len(state.Queue) > 0 && !occupied(state) }

func drain(next State, event Event) (State, error) {
	if event.CatchupWindow < 0 {
		return State{}, serviceerror.NewInvalidArgument("catchup window must be nonnegative")
	}
	if !NeedsDrain(next) {
		return next, nil
	}
	index := DrainIndex(next, event.Now, event.CatchupWindow)
	if index < 0 {
		next.Queue = nil
		return next, nil
	}
	if next.Root.Unfinished >= 4 {
		return next, nil
	}
	if len(event.Request) == 0 {
		return State{}, serviceerror.NewInvalidArgument("drain admission requires current request")
	}
	candidate := next.Queue[index]
	next.Queue = next.Queue[index+1:]
	if len(next.Queue) == 0 {
		next.Queue = nil
	}
	return startOperation(next, event, candidate.Nominal, candidate.WorkflowID, candidate.Policy)
}
