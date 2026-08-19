package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClassifyBufferedStart(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name  string
		start *schedulespb.BufferedStart
		want  BufferedStartState
	}{
		{name: "nil", want: BufferedStartStateInvalid},
		{name: "unknown negative attempt", start: &schedulespb.BufferedStart{Attempt: -2}, want: BufferedStartStateInvalid},
		{name: "unprocessed", start: unprocessedStart(), want: BufferedStartStateUnprocessed},
		{name: "deferred", start: deferredStart(), want: BufferedStartStateDeferred},
		{name: "ready without backoff", start: readyStart(), want: BufferedStartStateReady},
		{
			name: "ready before backoff boundary",
			start: func() *schedulespb.BufferedStart {
				start := readyStart()
				start.BackoffTime = timestamppb.New(now.Add(-time.Nanosecond))
				return start
			}(),
			want: BufferedStartStateReady,
		},
		{
			name: "ready at backoff boundary",
			start: func() *schedulespb.BufferedStart {
				start := readyStart()
				start.BackoffTime = timestamppb.New(now)
				return start
			}(),
			want: BufferedStartStateReady,
		},
		{name: "backing off", start: backingOffStart(now), want: BufferedStartStateBackingOff},
		{name: "started", start: runningStart(now), want: BufferedStartStateStarted},
		{name: "completed", start: completedStart(now), want: BufferedStartStateCompleted},
		{
			name: "completion before start",
			start: &schedulespb.BufferedStart{
				Attempt:   1,
				Completed: &schedulespb.CompletedResult{},
			},
			want: BufferedStartStateInvalid,
		},
		{
			name: "unprocessed with run ID",
			start: &schedulespb.BufferedStart{
				RunId: "run-id",
			},
			want: BufferedStartStateInvalid,
		},
		{
			name: "deferred with backoff",
			start: &schedulespb.BufferedStart{
				Attempt:     -1,
				BackoffTime: timestamppb.New(now),
			},
			want: BufferedStartStateInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ClassifyBufferedStart(tt.start, now))
		})
	}
}

func TestBufferedStartTransitions(t *testing.T) {
	now := time.Now()
	backoffTime := timestamppb.New(now.Add(time.Minute))
	startTime := timestamppb.New(now.Add(2 * time.Minute))
	completion := &schedulespb.CompletedResult{CloseTime: timestamppb.New(now.Add(3 * time.Minute))}
	start := &schedulespb.BufferedStart{RequestId: "request-id"}

	MarkStartDeferred(start)
	require.Equal(t, int64(-1), start.GetAttempt())
	require.Equal(t, "request-id", start.GetRequestId())
	require.Equal(t, BufferedStartStateDeferred, ClassifyBufferedStart(start, now))

	MarkStartUnprocessed(start)
	require.Zero(t, start.GetAttempt())
	require.Equal(t, BufferedStartStateUnprocessed, ClassifyBufferedStart(start, now))

	MarkStartReady(start)
	require.Equal(t, int64(1), start.GetAttempt())
	require.Equal(t, BufferedStartStateReady, ClassifyBufferedStart(start, now))

	MarkStartRetrying(start, 2, backoffTime)
	require.Equal(t, int64(2), start.GetAttempt())
	require.Same(t, backoffTime, start.GetBackoffTime())
	require.Equal(t, BufferedStartStateBackingOff, ClassifyBufferedStart(start, now))

	MarkStartStarted(start, "run-id", startTime)
	require.Equal(t, "run-id", start.GetRunId())
	require.Same(t, startTime, start.GetStartTime())
	require.Equal(t, int64(2), start.GetAttempt())
	require.Same(t, backoffTime, start.GetBackoffTime())
	require.Equal(t, BufferedStartStateStarted, ClassifyBufferedStart(start, now))

	MarkStartCompleted(start, completion)
	require.Same(t, completion, start.GetCompleted())
	require.Equal(t, "run-id", start.GetRunId())
	require.Equal(t, BufferedStartStateCompleted, ClassifyBufferedStart(start, now))
}

func unprocessedStart() *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{RequestId: "unprocessed"}
}

func deferredStart() *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{RequestId: "deferred", Attempt: -1}
}

func readyStart() *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{RequestId: "ready", Attempt: 1}
}

func backingOffStart(now time.Time) *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{
		RequestId:   "backing-off",
		Attempt:     2,
		BackoffTime: timestamppb.New(now.Add(time.Nanosecond)),
	}
}

func runningStart(now time.Time) *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{
		RequestId: "started",
		Attempt:   1,
		RunId:     "run-id",
		StartTime: timestamppb.New(now),
	}
}

func completedStart(now time.Time) *schedulespb.BufferedStart {
	start := runningStart(now)
	start.RequestId = "completed"
	start.Completed = &schedulespb.CompletedResult{CloseTime: timestamppb.New(now.Add(time.Minute))}
	return start
}
