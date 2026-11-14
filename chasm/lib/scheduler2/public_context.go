package scheduler2

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/contextutil"
)

// ContextMetadata attributes schedule API requests to the configured action.
func (s *Scheduler) ContextMetadata(ctx chasm.Context) map[string]string {
	var workflowType, taskQueue string
	if s.OperationStore != nil {
		metadata := s.PublicMetadata.Get(ctx)
		workflowType, taskQueue = metadata.GetWorkflowType(), metadata.GetTaskQueue()
	} else {
		schedule, ok := s.PublicSchedule.TryGet(ctx)
		if !ok {
			return nil
		}
		action := schedule.GetAction().GetStartWorkflow()
		workflowType, taskQueue = action.GetWorkflowType().GetName(), action.GetTaskQueue().GetName()
	}
	metadata := make(map[string]string, 2)
	if name := workflowType; name != "" {
		metadata[contextutil.MetadataKeyWorkflowType] = name
	}
	if queue := taskQueue; queue != "" {
		metadata[contextutil.MetadataKeyWorkflowTaskQueue] = queue
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}
