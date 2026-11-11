package scheduler2

import (
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"google.golang.org/protobuf/types/known/durationpb"
)

// NewExecution anchors a scheduler to the engine-allocated run and execution clock.
func NewExecution(ctx chasm.MutableContext, remaining int64) (*Scheduler, error) {
	if ctx.ExecutionKey().RunID == "" {
		return nil, serviceerror.NewInvalidArgument("scheduler requires allocated execution run ID")
	}
	s := New(ctx.ExecutionKey().RunID, remaining)
	at, err := activityNano(ctx.Now(s))
	if err != nil {
		return nil, err
	}
	s.ActivitySet, s.ActivityAt = true, at
	return s, nil
}

func activityNano(at time.Time) (int64, error) {
	nano := at.UnixNano()
	if !time.Unix(0, nano).Equal(at) {
		return 0, serviceerror.NewInvalidArgument("activity time exceeds nanosecond range")
	}
	return nano, nil
}

type startSnapshot struct {
	request *workflowservice.StartWorkflowExecutionRequest
	skipped time.Duration
}

func (s *Scheduler) startSnapshot(ctx chasm.Context, id string) (startSnapshot, error) {
	request, err := s.request(ctx, id)
	if err != nil || request == nil {
		return startSnapshot{}, err
	}
	_, propagated := ctx.GetTimeSkippingPropagateState()
	skipped := propagated.GetInitialSkippedDuration()
	if skipped != nil {
		if err := skipped.CheckValid(); err != nil {
			return startSnapshot{}, serviceerror.NewInvalidArgument("invalid skipped duration")
		}
		exact := durationpb.New(skipped.AsDuration())
		if exact.Seconds != skipped.Seconds || exact.Nanos != skipped.Nanos {
			return startSnapshot{}, serviceerror.NewInvalidArgument("skipped duration exceeds nanosecond range")
		}
	}
	return startSnapshot{request: request, skipped: skipped.AsDuration()}, nil
}
