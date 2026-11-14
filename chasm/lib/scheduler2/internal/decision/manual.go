package decision

import (
	"math"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// ManualCommand retains accepted identity independently of execution capacity.
type ManualCommand struct {
	NominalTime Timestamp
	ID          uint64
	Nominal     int64
	Policy      enumspb.ScheduleOverlapPolicy
}

func acceptManual(next State, event Event) (State, error) {
	if !ValidPolicy(event.Policy) {
		return State{}, serviceerror.NewInvalidArgument("unsupported overlap policy")
	}
	limit := 4
	if event.PublicManualIngress {
		limit = 100
	}
	if len(next.Manuals) >= limit {
		if event.PublicManualIngress {
			return State{}, serviceerror.NewFailedPrecondition("too many concurrent backfillers")
		}
		return State{}, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "pending manual trigger capacity exhausted")
	}
	if next.Root.ManualSequence == math.MaxUint64 {
		return State{}, serviceerror.NewFailedPrecondition("manual trigger sequence exhausted")
	}
	next.Root.ManualSequence++
	next.Manuals = append(next.Manuals, ManualCommand{ID: next.Root.ManualSequence, Nominal: event.Nominal, NominalTime: event.NominalTime, Policy: event.Policy})
	return next, nil
}

func manualPolicy(state State) enumspb.ScheduleOverlapPolicy {
	policy := state.Manuals[0].Policy
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = state.Root.Policy
	}
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return policy
}

// CanDispatchManual excludes external queue and operation capacity waits.
func CanDispatchManual(state State) bool {
	if len(state.Manuals) == 0 || !QueueAvailable(state) {
		return false
	}
	if manualPolicy(state) != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL && (occupied(state) || QueueLength(state) > 0) {
		return true
	}
	return OperationAvailable(state)
}

func dispatchManual(next State, event Event) (State, error) {
	if !CanDispatchManual(next) {
		return next, nil
	}
	command := next.Manuals[0]
	policy := manualPolicy(next)
	next.Manuals = next.Manuals[1:]
	if policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL && (occupied(next) || QueueLength(next) > 0) {
		if policy == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP {
			return next, nil
		}
		if policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE && QueueLength(next) > 0 {
			return next, nil
		}
		if event.WorkflowID == "" {
			return State{}, serviceerror.NewInvalidArgument("manual queue requires workflow identity")
		}
		return appendQueued(next, Queued{Nominal: command.Nominal, Actual: command.Nominal, NominalTime: command.NominalTime, ActualTime: command.NominalTime, WorkflowID: event.WorkflowID, Policy: policy, ManualID: command.ID})
	}
	if len(event.Request) == 0 || next.Root.Incarnation == "" {
		return State{}, serviceerror.NewInvalidArgument("manual admission requires incarnation and request")
	}
	return startOperation(next, event, Queued{Nominal: command.Nominal, NominalTime: command.NominalTime, WorkflowID: event.WorkflowID, Policy: policy, ManualID: command.ID})
}

// ManualNeedsTemplate distinguishes identity/admission work from policy-only disposal.
func ManualNeedsTemplate(state State) bool {
	if !CanDispatchManual(state) {
		return false
	}
	policy := manualPolicy(state)
	if occupied(state) || QueueLength(state) > 0 {
		return policy != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP && (policy != enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || QueueLength(state) == 0)
	}
	return true
}

func dispatchBackfill(next State, event Event) (State, error) {
	if event.BackfillID == 0 || !ValidPolicy(event.Policy) {
		return State{}, serviceerror.NewInvalidArgument("invalid backfill occurrence")
	}
	if !QueueAvailable(next) {
		return next, nil
	}
	policy := event.Policy
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = next.Root.Policy
	}
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	if event.BackfillBatchSkip {
		if policy != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP {
			return State{}, serviceerror.NewInvalidArgument("batch skip requires SKIP policy")
		}
		return next, nil
	}
	candidate := Queued{BackfillID: event.BackfillID, Nominal: event.Nominal, Actual: event.Actual, NominalTime: event.NominalTime, ActualTime: event.ActualTime, WorkflowID: event.WorkflowID, Policy: policy}
	if policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL && (occupied(next) || QueueLength(next) > 0) {
		if policy == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP || (policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE && QueueLength(next) > 0) {
			return next, nil
		}
		if event.WorkflowID == "" {
			return State{}, serviceerror.NewInvalidArgument("backfill queue requires workflow identity")
		}
		return appendQueued(next, candidate)
	}
	if !OperationAvailable(next) {
		return next, nil
	}
	if len(event.Request) == 0 || next.Root.Incarnation == "" {
		return State{}, serviceerror.NewInvalidArgument("backfill admission requires incarnation and request")
	}
	return startOperation(next, event, candidate)
}
