package rpcgen

import (
	"context"
	"time"

	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

// Behavior is a replayable outbound unary RPC outcome. Profiles choose which
// behavior is legal for a method; this package does not infer that from a
// descriptor.
type Behavior[Request, Response proto.Message] struct {
	Label     string
	Response  Response
	Err       error
	Delay     time.Duration
	Committed bool
}

// Queue installs behavior on a typed generated-mock script.
func (b Behavior[Request, Response]) Queue(script *rpctest.Script[Request, Response]) {
	script.PushContext(b.Label, func(ctx context.Context, _ Request) (Response, error) {
		if b.Delay > 0 {
			timer := time.NewTimer(b.Delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				var response Response
				return response, ctx.Err()
			case <-timer.C:
			}
		}
		return b.Response, b.Err
	})
}

// Success returns a successful response behavior.
func Success[Request, Response proto.Message](response Response) Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "success", Response: response}
}

// Retryable returns a retryable gRPC failure behavior.
func Retryable[Request, Response proto.Message](code codes.Code) Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "retryable-" + code.String(), Err: status.Error(code, "injected retryable failure")}
}

// Terminal returns a terminal gRPC failure behavior.
func Terminal[Request, Response proto.Message](code codes.Code) Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "terminal-" + code.String(), Err: status.Error(code, "injected terminal failure")}
}

// Timeout waits for the caller's deadline or cancellation before returning.
func Timeout[Request, Response proto.Message]() Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "timeout", Delay: time.Hour}
}

// Cancellation returns the cancellation observed by an already-canceled caller.
func Cancellation[Request, Response proto.Message]() Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "cancellation", Err: context.Canceled}
}

// AmbiguousCommit records that the dependency committed response before the
// caller observed an unavailable error.
func AmbiguousCommit[Request, Response proto.Message](response Response) Behavior[Request, Response] {
	return Behavior[Request, Response]{
		Label:     "ambiguous-commit",
		Response:  response,
		Err:       status.Error(codes.Unavailable, "response lost after commit"),
		Committed: true,
	}
}

// RetrySequence queues a finite retry sequence in call order.
func RetrySequence[Request, Response proto.Message](behaviors ...Behavior[Request, Response]) []Behavior[Request, Response] {
	return behaviors
}

// QueueSequence installs a retry sequence in call order.
func QueueSequence[Request, Response proto.Message](
	script *rpctest.Script[Request, Response],
	behaviors ...Behavior[Request, Response],
) {
	for _, behavior := range behaviors {
		behavior.Queue(script)
	}
}

// Draw selects a behavior on the property-test goroutine. The supplied label
// is retained in Rapid's replay trace and the selected behavior label is kept
// in the mock call history.
func Draw[Request, Response proto.Message](
	t *rapid.T,
	label string,
	behaviors ...Behavior[Request, Response],
) Behavior[Request, Response] {
	if len(behaviors) == 0 {
		t.Fatalf("rpcgen: Draw requires at least one behavior")
	}
	return behaviors[rapid.IntRange(0, len(behaviors)-1).Draw(t, label)]
}
