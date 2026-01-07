package scheduler

import (
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	commonscheduler "go.temporal.io/server/common/scheduler"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func generateRequestID(scheduler *Scheduler, backfillID string, nominal, actual time.Time) string {
	return commonscheduler.GenerateRequestID(commonscheduler.RequestIDParams{
		BackfillID:    backfillID,
		NamespaceID:   scheduler.NamespaceId,
		ScheduleID:    scheduler.ScheduleId,
		ConflictToken: scheduler.ConflictToken,
		NominalTime:   nominal,
		ActualTime:    actual,
	})
}

// newTaggedLogger returns a logger tagged with the Scheduler's attributes.
func newTaggedLogger(baseLogger log.Logger, scheduler *Scheduler) log.Logger {
	return log.With(
		baseLogger,
		tag.WorkflowNamespace(scheduler.Namespace),
		tag.ScheduleID(scheduler.ScheduleId),
	)
}

// validateTaskHighWaterMark validates a component's lastProcessedTime against a
// task timestamp, returning ErrStaleReference for out-of-date tasks.
func validateTaskHighWaterMark(lastProcessedTime *timestamppb.Timestamp, scheduledAt time.Time) (bool, error) {
	if lastProcessedTime != nil {
		hwm := lastProcessedTime.AsTime()

		if hwm.After(scheduledAt) || hwm.Equal(scheduledAt) {
			return false, nil
		}
	}

	return true, nil
}

// jsonStringer wraps a proto.Message for lazy JSON serialization. Intended for
// debug logging structures.
type jsonStringer struct {
	proto.Message
}

func (j jsonStringer) String() string {
	json, _ := protojson.Marshal(j.Message)
	return string(json)
}
