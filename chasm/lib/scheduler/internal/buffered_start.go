package internal

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
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

const (
	bufferedStartDeferredAttempt       int64 = -1
	bufferedStartUnprocessedAttempt    int64 = 0
	bufferedStartFirstExecutionAttempt int64 = 1
)

// ClassifyBufferedStart returns the lifecycle state encoded by start at retryEvaluationTime.
func ClassifyBufferedStart(
	start *schedulespb.BufferedStart,
	retryEvaluationTime time.Time,
) BufferedStartState {
	if start == nil || start.GetAttempt() < bufferedStartDeferredAttempt {
		return BufferedStartStateInvalid
	}

	hasRun := RunID(start) != ""
	hasCompletion := IsCompleted(start)
	hasBackoff := start.GetBackoffTime() != nil

	switch start.GetAttempt() {
	case bufferedStartDeferredAttempt:
		if hasRun || hasCompletion || hasBackoff {
			return BufferedStartStateInvalid
		}
		return BufferedStartStateDeferred
	case bufferedStartUnprocessedAttempt:
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
	start.Attempt = bufferedStartUnprocessedAttempt
}

// MarkStartMigratedUnprocessed clears V2 retry state from a pending V1 start.
func MarkStartMigratedUnprocessed(start *schedulespb.BufferedStart) {
	MarkStartUnprocessed(start)
	start.BackoffTime = nil
}

// MarkStartDeferred marks a start as waiting for an overlapping action to complete.
func MarkStartDeferred(start *schedulespb.BufferedStart) {
	start.Attempt = bufferedStartDeferredAttempt
}

// MarkStartReady marks a start as ready for its first execution attempt.
func MarkStartReady(start *schedulespb.BufferedStart) {
	start.Attempt = bufferedStartFirstExecutionAttempt
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
	if start.GetExecution() != nil {
		start.Execution.RunId = runID
		if start.Execution.GetType() == enumspb.EXECUTION_TYPE_WORKFLOW {
			start.RunId = runID
		}
	} else {
		start.RunId = runID
	}
	start.StartTime = startTime
	if start.GetCompletion() != nil {
		start.Completion.Execution = Execution(start)
	}
}

// MarkStartCompleted records a start's terminal workflow result.
func MarkStartCompleted(start *schedulespb.BufferedStart, result *schedulespb.CompletedResult) {
	start.Completed = result
}
