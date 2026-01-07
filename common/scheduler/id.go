package scheduler

import (
	"fmt"
	"time"
)

// RequestIDParams contains parameters for generating a request ID.
type RequestIDParams struct {
	BackfillID    string // "auto" for spec-driven actions, unique ID for backfills/triggers
	NamespaceID   string
	ScheduleID    string
	ConflictToken int64
	NominalTime   time.Time
	ActualTime    time.Time
}

// GenerateRequestID generates a deterministic request ID for a buffered action.
// The request ID is deterministic because the jittered actual time (as well as
// the spec's nominal time) is also deterministic.
func GenerateRequestID(params RequestIDParams) string {
	backfillID := params.BackfillID
	if backfillID == "" {
		backfillID = "auto"
	}
	return fmt.Sprintf(
		"sched-%s-%s-%s-%d-%d-%d",
		backfillID,
		params.NamespaceID,
		params.ScheduleID,
		params.ConflictToken,
		params.NominalTime.UnixMilli(),
		params.ActualTime.UnixMilli(),
	)
}

// WorkflowIDParams contains parameters for generating a workflow ID.
type WorkflowIDParams struct {
	BaseWorkflowID string    // From the schedule spec's action
	NominalTime    time.Time // Scheduled time for the action
}

// GenerateWorkflowID generates a workflow ID for a scheduled action.
// The nominal time is truncated to seconds and formatted as RFC3339.
func GenerateWorkflowID(params WorkflowIDParams) string {
	nominalTimeSec := params.NominalTime.Truncate(time.Second)
	return fmt.Sprintf("%s-%s", params.BaseWorkflowID, nominalTimeSec.UTC().Format(time.RFC3339))
}
