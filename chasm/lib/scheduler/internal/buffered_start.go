package internal

import (
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BufferedStartState identifies the lifecycle state encoded by a BufferedStart's fields.
type BufferedStartState int

const (
	BufferedStartStateInvalid BufferedStartState = iota
	BufferedStartStateUnprocessed
	BufferedStartStateDeferred
	BufferedStartStateReady
	BufferedStartStateBackingOff
	BufferedStartStateStarted
	BufferedStartStateCompleted
)

// ClassifyBufferedStart returns the lifecycle state encoded by start at retryEvaluationTime.
func ClassifyBufferedStart(
	start *schedulespb.BufferedStart,
	retryEvaluationTime time.Time,
) BufferedStartState {
	if start == nil || start.GetAttempt() < -1 {
		return BufferedStartStateInvalid
	}

	hasRun := start.GetRunId() != ""
	hasCompletion := start.GetCompleted() != nil
	hasBackoff := start.GetBackoffTime() != nil

	switch start.GetAttempt() {
	case -1:
		if hasRun || hasCompletion || hasBackoff {
			return BufferedStartStateInvalid
		}
		return BufferedStartStateDeferred
	case 0:
		if hasRun || hasCompletion || hasBackoff {
			return BufferedStartStateInvalid
		}
		return BufferedStartStateUnprocessed
	default:
		if hasRun {
			if hasCompletion {
				return BufferedStartStateCompleted
			}
			return BufferedStartStateStarted
		}
		if hasCompletion {
			return BufferedStartStateInvalid
		}
		if hasBackoff && start.GetBackoffTime().AsTime().After(retryEvaluationTime) {
			return BufferedStartStateBackingOff
		}
		return BufferedStartStateReady
	}
}

// MarkStartUnprocessed marks a start for its initial overlap-policy pass.
func MarkStartUnprocessed(start *schedulespb.BufferedStart) {
	start.Attempt = 0
}

// MarkStartMigratedUnprocessed clears V2 retry state from a pending V1 start.
func MarkStartMigratedUnprocessed(start *schedulespb.BufferedStart) {
	MarkStartUnprocessed(start)
	start.BackoffTime = nil
}

// MarkStartDeferred marks a start as waiting for an overlapping action to complete.
func MarkStartDeferred(start *schedulespb.BufferedStart) {
	start.Attempt = -1
}

// MarkStartReady marks a start as ready for its first execution attempt.
func MarkStartReady(start *schedulespb.BufferedStart) {
	start.Attempt = 1
}

// MarkStartRetrying advances a start to its next attempt and backoff deadline.
func MarkStartRetrying(
	start *schedulespb.BufferedStart,
	nextAttempt int64,
	backoffTime *timestamppb.Timestamp,
) {
	start.Attempt = nextAttempt
	start.BackoffTime = backoffTime
}

// MarkStartStarted records the execution created for a start.
func MarkStartStarted(
	start *schedulespb.BufferedStart,
	runID string,
	startTime *timestamppb.Timestamp,
) {
	start.RunId = runID
	start.StartTime = startTime
}

// MarkStartCompleted records a start's terminal workflow result.
func MarkStartCompleted(start *schedulespb.BufferedStart, result *schedulespb.CompletedResult) {
	start.Completed = result
}
