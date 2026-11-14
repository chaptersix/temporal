package scheduler2

import (
	"time"

	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

var (
	publicIdleCloseTime        = chasm.NewSearchAttributeDateTime(schedulerconfig.ScheduleIdleCloseTimeName, chasm.SearchAttributeFieldDateTime02)
	publicNextActionTime       = chasm.NewSearchAttributeDateTime(schedulerconfig.ScheduleNextActionTimeName, chasm.SearchAttributeFieldDateTime01)
	publicRunningWorkflowCount = chasm.NewSearchAttributeInt(schedulerconfig.ScheduleRunningWorkflowCountName, chasm.SearchAttributeFieldInt01)
	publicBufferedStartsCount  = chasm.NewSearchAttributeInt(schedulerconfig.ScheduleBufferedStartsCountName, chasm.SearchAttributeFieldInt02)
)

func (s *Scheduler) SearchAttributes(ctx chasm.Context) []chasm.SearchAttributeKeyValue {
	attributes := []chasm.SearchAttributeKeyValue{chasm.SearchAttributeTemporalSchedulePaused.Value(s.Paused)}
	if s.Closed {
		return attributes
	}
	// The list preview is refreshed by accepted transitions using the live spec
	// builder. Indexing reads its first entry without loading action payloads.
	if list, ok := s.PublicList.TryGet(ctx); ok && len(list.FutureActionTimes) > 0 {
		attributes = append(attributes, publicNextActionTime.Value(list.FutureActionTimes[0].AsTime()))
	}
	if s.IdleArmed {
		attributes = append(attributes, publicIdleCloseTime.Value(time.Unix(0, s.IdleAt)))
	}
	var running int64
	if s.ProjectionStore != nil {
		if err := s.validatePublicProjection(ctx); err != nil {
			// The framework caller handles invariant failures, as with DATA decoding.
			panic(err) //nolint:forbidigo
		}
		running = s.ProjectionStore.RunningCount
	} else if runtime, ok := s.PublicRuntime.TryGet(ctx); ok {
		for _, action := range runtime.Actions {
			operation, exists := s.Operations[action.OperationId]
			if exists && operation.Get(ctx).Phase == scheduler2pb.PHASE_STARTED && action.Result.GetStartWorkflowResult().GetRunId() != "" {
				running++
			}
		}
	}
	// Successful start acknowledgement separates buffered and running actions,
	// matching Describe and the existing scheduler visibility contract.
	return append(attributes, publicRunningWorkflowCount.Value(running), publicBufferedStartsCount.Value(s.queueLength()+s.UnconfirmedStarts))
}
