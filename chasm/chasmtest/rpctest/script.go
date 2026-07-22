package rpctest

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Responder returns one scripted unary RPC outcome.
type Responder[Request, Response proto.Message] func(Request) (Response, error)

// Call is an immutable snapshot of a completed scripted RPC.
type Call[Request, Response proto.Message] struct {
	Name     string
	Request  Request
	Response Response
	Err      error
}

type outcome[Request, Response proto.Message] struct {
	name      string
	responder Responder[Request, Response]
}

// Script supplies ordered, typed outcomes to a generated unary client mock.
// Its zero value is ready for use.
type Script[Request, Response proto.Message] struct {
	mu             sync.Mutex
	queued         []outcome[Request, Response]
	defaultOutcome *outcome[Request, Response]
	calls          []Call[Request, Response]
}

// Push appends an outcome to the script.
func (s *Script[Request, Response]) Push(
	name string,
	responder Responder[Request, Response],
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queued = append(s.queued, outcome[Request, Response]{name: name, responder: responder})
}

// SetDefault sets the deterministic outcome used after the queue is exhausted.
func (s *Script[Request, Response]) SetDefault(
	name string,
	responder Responder[Request, Response],
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultOutcome = &outcome[Request, Response]{name: name, responder: responder}
}

// Handle has the signature expected by generated unary gRPC client mocks.
func (s *Script[Request, Response]) Handle(
	_ context.Context,
	request Request,
	_ ...grpc.CallOption,
) (Response, error) {
	request = clone(request)

	s.mu.Lock()
	defer s.mu.Unlock()
	var selected outcome[Request, Response]
	switch {
	case len(s.queued) > 0:
		selected = s.queued[0]
		s.queued = s.queued[1:]
	case s.defaultOutcome != nil:
		selected = *s.defaultOutcome
	default:
		var response Response
		err := fmt.Errorf("rpctest: no response configured for request %T", request)
		s.calls = append(s.calls, Call[Request, Response]{Request: request, Err: err})
		return response, err
	}
	response, err := selected.responder(request)
	response = clone(response)
	s.calls = append(s.calls, Call[Request, Response]{
		Name:     selected.name,
		Request:  request,
		Response: clone(response),
		Err:      err,
	})
	return response, err
}

// Calls returns cloned snapshots in invocation order.
func (s *Script[Request, Response]) Calls() []Call[Request, Response] {
	s.mu.Lock()
	defer s.mu.Unlock()

	calls := make([]Call[Request, Response], 0, len(s.calls))
	for _, entry := range s.calls {
		calls = append(calls, Call[Request, Response]{
			Name:     entry.Name,
			Request:  clone(entry.Request),
			Response: clone(entry.Response),
			Err:      entry.Err,
		})
	}
	return calls
}

// Pending reports the number of queued outcomes that have not been consumed.
func (s *Script[Request, Response]) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queued)
}

// Return creates a responder that always returns response.
func Return[Request, Response proto.Message](response Response) Responder[Request, Response] {
	return func(Request) (Response, error) {
		return response, nil
	}
}

// Fail creates a responder that always returns err.
func Fail[Request, Response proto.Message](err error) Responder[Request, Response] {
	return func(Request) (Response, error) {
		var response Response
		return response, err
	}
}

func clone[Message proto.Message](message Message) Message {
	value := reflect.ValueOf(message)
	if !value.IsValid() || value.Kind() == reflect.Ptr && value.IsNil() {
		return message
	}
	return proto.CloneOf(message)
}
