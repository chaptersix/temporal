package decision

import (
	"bytes"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

const MaxCompletionBytes = 64 * 1024

type Outcome int

const (
	NoOutcome Outcome = iota
	Success
	Failure
)

// Carryover preserves opaque serialized payload and failure values for future admission.
type Carryover struct {
	Result         []byte
	Failure        []byte
	FailurePresent bool
}

func complete(next *State, event Event) error {
	if err := validateCompletion(event); err != nil {
		return err
	}
	if event.Kind == Callback {
		if next.Carryover == nil {
			next.Carryover = &Carryover{}
		}
		if event.Outcome == Success {
			next.Carryover.Result = bytes.Clone(event.Result)
			next.Carryover.Failure = nil
			next.Carryover.FailurePresent = false
		} else {
			next.Carryover.Failure = bytes.Clone(event.Failure)
			next.Carryover.FailurePresent = true
		}
		next.Root.CarryoverSet = true
	}
	if next.Root.PauseOnFailure && (event.Status == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED || event.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT) {
		next.Root.Paused = true
	}
	return nil
}

func validateCompletion(event Event) error {
	if len(event.Result) > MaxCompletionBytes || len(event.Failure) > MaxCompletionBytes {
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "completion value exceeds prototype limit")
	}
	status := event.Status
	if event.Kind == Complete && event.Outcome == NoOutcome && len(event.Result)+len(event.Failure) == 0 && status == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		return nil
	}
	terminal := status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED || status == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED || status == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT || status == enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED || status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	if !terminal {
		return serviceerror.NewInvalidArgument("completion requires terminal workflow status")
	}
	if event.Kind == Complete {
		if event.Outcome != NoOutcome || len(event.Result)+len(event.Failure) != 0 {
			return serviceerror.NewInvalidArgument("metadata completion cannot contain an outcome")
		}
		return nil
	}
	if event.Outcome == Success && status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED && len(event.Failure) == 0 {
		return nil
	}
	if event.Outcome == Failure && status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED && len(event.Result) == 0 {
		return nil
	}
	return serviceerror.NewInvalidArgument("callback outcome does not match terminal status")
}
