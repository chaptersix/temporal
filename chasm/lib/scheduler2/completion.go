package scheduler2

import (
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
)

// prepareCompletion validates opaque values before mutating the CHASM tree.
// Unmatched and already completed callbacks do not prepare payload partitions.
func (s *Scheduler) prepareCompletion(state decision.State, event *decision.Event) error {
	op := state.Operation
	if event.Kind != decision.Callback || event.Incarnation != s.Incarnation || op == nil || op.ID != event.OperationID || op.Phase == decision.Completed {
		return nil
	}
	if len(event.Result) > decision.MaxCompletionBytes || len(event.Failure) > decision.MaxCompletionBytes {
		return nil
	} // The reducer reports the bounded-input error.
	if event.Outcome == decision.Success {
		payload := &commonpb.Payload{}
		if err := proto.Unmarshal(event.Result, payload); err != nil {
			return serviceerror.NewInvalidArgument("invalid completion result")
		}
		// Empty payload means no result; encoding metadata makes explicit null present.
		if proto.Size(payload) == 0 {
			event.Result = nil
		} else {
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
			if err != nil {
				return err
			}
			event.Result = encoded
		}
	}
	if event.Outcome == decision.Failure {
		failure := &failurepb.Failure{}
		if err := proto.Unmarshal(event.Failure, failure); err != nil {
			return serviceerror.NewInvalidArgument("invalid completion failure")
		}
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(failure)
		if err != nil {
			return err
		}
		event.Failure = encoded
	}
	return nil
}

func (s *Scheduler) materializeCarryover(ctx chasm.Context, request *workflowservice.StartWorkflowExecutionRequest) error {
	c := s.Carryover.Get(ctx)
	request.LastCompletionResult = nil
	request.ContinuedFailure = nil
	if len(c.Result) > 0 {
		result := &commonpb.Payload{}
		if err := proto.Unmarshal(c.Result, result); err != nil {
			return err
		}
		request.LastCompletionResult = &commonpb.Payloads{Payloads: []*commonpb.Payload{result}}
	}
	if c.FailurePresent {
		failure := &failurepb.Failure{}
		if err := proto.Unmarshal(c.Failure, failure); err != nil {
			return err
		}
		request.ContinuedFailure = failure
	}
	return nil
}
