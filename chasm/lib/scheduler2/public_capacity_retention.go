package scheduler2

import (
	"slices"

	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// retainPublicCompletions matches the reference buffer's completed-result
// occupancy, independently of recent actions ordered by start time.
func retainPublicCompletions(runtime *scheduler2pb.PublicRuntime, before, next decision.State, event decision.Event) string {
	op := next.Operation
	if op == nil || op.Phase != decision.Completed || (before.Operation != nil && before.Operation.Phase == decision.Completed) {
		return ""
	}
	runtime.CompletedOperations = append(runtime.CompletedOperations, &scheduler2pb.PublicCompletedOperation{OperationId: op.ID, CloseTime: timestamppb.New(event.PublicCloseTime)})
	slices.SortStableFunc(runtime.CompletedOperations, func(a, b *scheduler2pb.PublicCompletedOperation) int {
		return a.CloseTime.AsTime().Compare(b.CloseTime.AsTime())
	})
	if len(runtime.CompletedOperations) > 10 {
		oldest := runtime.CompletedOperations[0]
		runtime.CompletedOperations = runtime.CompletedOperations[1:]
		return oldest.OperationId
	}
	return ""
}
