package scheduler2

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/contextutil"
)

// ContextMetadata attributes schedule API requests to the configured action.
func (s *Scheduler) ContextMetadata(ctx chasm.Context) map[string]string {
	schedule, ok := s.PublicSchedule.TryGet(ctx)
	if !ok {
		return nil
	}
	action := schedule.GetAction().GetStartWorkflow()
	metadata := make(map[string]string, 2)
	if name := action.GetWorkflowType().GetName(); name != "" {
		metadata[contextutil.MetadataKeyWorkflowType] = name
	}
	if queue := action.GetTaskQueue().GetName(); queue != "" {
		metadata[contextutil.MetadataKeyWorkflowTaskQueue] = queue
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}
