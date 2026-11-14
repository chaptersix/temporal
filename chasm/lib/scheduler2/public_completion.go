package scheduler2

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
)

var _ chasm.NexusCompletionHandler = (*Scheduler)(nil)

// HandleNexusCompletion applies the actual workflow completion to its frozen
// admission. History validates the component reference before invoking this method.
func (s *Scheduler) HandleNexusCompletion(ctx chasm.MutableContext, completion *persistencespb.ChasmNexusCompletion) error {
	if s.Closed {
		return nil
	}
	if err := s.validateOperationStore(); err != nil {
		return err
	}
	operation, ok, err := s.operationRecord(ctx, completion.GetRequestId())
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if decision.Terminal(decision.Phase(operation.Phase)) {
		return nil
	}
	chain := completion.GetWorkflowFirstExecutionRunId()
	if chain == "" {
		return serviceerror.NewInvalidArgument("workflow completion requires first execution run ID")
	}
	if completion.CloseTime == nil || completion.CloseTime.CheckValid() != nil {
		return serviceerror.NewInvalidArgument("workflow completion requires valid close time")
	}
	event := decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: completion.RequestId, ChainID: chain, PublicCloseTime: completion.GetCloseTime().AsTime()}
	switch outcome := completion.GetOutcome().(type) {
	case *persistencespb.ChasmNexusCompletion_Success:
		event.Outcome, event.Status = decision.Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		event.Result, err = proto.MarshalOptions{Deterministic: true}.Marshal(outcome.Success)
	case *persistencespb.ChasmNexusCompletion_Failure:
		if outcome.Failure == nil {
			return serviceerror.NewInvalidArgument("invalid completion failure")
		}
		event.Outcome, event.Status = decision.Failure, completionFailureStatus(outcome.Failure)
		event.Failure, err = proto.MarshalOptions{Deterministic: true}.Marshal(outcome.Failure)
	default:
		return serviceerror.NewInvalidArgument("invalid completion outcome")
	}
	if err != nil {
		return err
	}
	return s.Apply(ctx, event)
}
