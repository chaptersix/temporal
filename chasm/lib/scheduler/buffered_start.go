package scheduler

import (
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerinternal "go.temporal.io/server/chasm/lib/scheduler/internal"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type bufferedStartState int

const (
	bufferedStartStateInvalid bufferedStartState = iota
	bufferedStartStateUnprocessed
	bufferedStartStateDeferred
	bufferedStartStateReady
	bufferedStartStateBackingOff
	bufferedStartStateStarted
	bufferedStartStateCompleted
)

func classifyBufferedStart(
	start *schedulespb.BufferedStart,
	retryEvaluationTime time.Time,
) bufferedStartState {
	if start == nil || start.GetAttempt() < -1 {
		return bufferedStartStateInvalid
	}

	hasRun := start.GetRunId() != ""
	hasCompletion := start.GetCompleted() != nil
	hasBackoff := start.GetBackoffTime() != nil

	switch start.GetAttempt() {
	case -1:
		if hasRun || hasCompletion || hasBackoff {
			return bufferedStartStateInvalid
		}
		return bufferedStartStateDeferred
	case 0:
		if hasRun || hasCompletion || hasBackoff {
			return bufferedStartStateInvalid
		}
		return bufferedStartStateUnprocessed
	default:
		if hasRun {
			if hasCompletion {
				return bufferedStartStateCompleted
			}
			return bufferedStartStateStarted
		}
		if hasCompletion {
			return bufferedStartStateInvalid
		}
		if hasBackoff && start.GetBackoffTime().AsTime().After(retryEvaluationTime) {
			return bufferedStartStateBackingOff
		}
		return bufferedStartStateReady
	}
}

func markStartUnprocessed(start *schedulespb.BufferedStart) {
	schedulerinternal.MarkStartUnprocessed(start)
}

func markStartDeferred(start *schedulespb.BufferedStart) {
	schedulerinternal.MarkStartDeferred(start)
}

func markStartReady(start *schedulespb.BufferedStart) {
	schedulerinternal.MarkStartReady(start)
}

func markStartRetrying(
	start *schedulespb.BufferedStart,
	nextAttempt int64,
	backoffTime *timestamppb.Timestamp,
) {
	schedulerinternal.MarkStartRetrying(start, nextAttempt, backoffTime)
}

func markStartStarted(
	start *schedulespb.BufferedStart,
	runID string,
	startTime *timestamppb.Timestamp,
) {
	schedulerinternal.MarkStartStarted(start, runID, startTime)
}

func markStartCompleted(start *schedulespb.BufferedStart, result *schedulespb.CompletedResult) {
	schedulerinternal.MarkStartCompleted(start, result)
}
