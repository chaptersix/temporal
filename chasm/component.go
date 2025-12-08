//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination component_mock.go

package chasm

import (
	"context"
	"reflect"
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
)

// Component represents a stateful entity with behavior and lifecycle in the CHASM framework.
// Components are organized in a tree structure where each component maintains its own state
// through Field[T] values and can have child components. Components implement business logic
// through custom methods invoked via Engine operations.
//
// Components have a lifecycle (Running, Completed, Failed) managed through LifecycleState.
// Once a component reaches a closed state (Completed or Failed), it cannot transition back
// to an open state.
//
// To implement a component, define a struct that embeds UnimplementedComponent, add Field[T]
// values for state, implement custom methods for state transitions, and register the component
// type with a Library.
type Component interface {
	LifecycleState(Context) LifecycleState

	Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error)

	// we may not need this in the beginning
	mustEmbedUnimplementedComponent()
}

// TerminateComponentRequest contains parameters for terminating a component.
type TerminateComponentRequest struct {
	Identity string
	Reason   string
	Details  *commonpb.Payloads
}

// TerminateComponentResponse is returned from Component.Terminate.
type TerminateComponentResponse struct{}

// Embed UnimplementedComponent to get forward compatibility
type UnimplementedComponent struct{}

func (UnimplementedComponent) Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error) {
	return TerminateComponentResponse{}, nil
}

func (UnimplementedComponent) mustEmbedUnimplementedComponent() {}

var UnimplementedComponentT = reflect.TypeFor[UnimplementedComponent]()

// LifecycleState represents the current lifecycle state of a Component.
//
// Components progress through lifecycle states from open states (Running) to closed states
// (Completed or Failed). Once a component reaches a closed state, it cannot transition back
// to an open state.
//
// Use IsClosed() to check if a component is in a terminal state.
type LifecycleState int

const (
	// Lifecycle states that are considered OPEN
	//
	// LifecycleStateCreated LifecycleState = 1 << iota

	// LifecycleStateRunning indicates the component is actively processing and can accept transitions.
	LifecycleStateRunning LifecycleState = 2 << iota
	// LifecycleStatePaused

	// Lifecycle states that are considered CLOSED
	//

	// LifecycleStateCompleted indicates the component finished successfully.
	// This is a terminal state.
	LifecycleStateCompleted

	// LifecycleStateFailed indicates the component terminated with an error.
	// This is a terminal state.
	LifecycleStateFailed
	// LifecycleStateTerminated
	// LifecycleStateTimedout
	// LifecycleStateReset
)

// IsClosed returns true if the lifecycle state is terminal (Completed or Failed).
//
// Components in a closed state cannot transition back to an open state and should not
// accept further state mutations.
func (s LifecycleState) IsClosed() bool {
	return s >= LifecycleStateCompleted
}

func (s LifecycleState) String() string {
	switch s {
	case LifecycleStateRunning:
		return "Running"
	case LifecycleStateCompleted:
		return "Completed"
	case LifecycleStateFailed:
		return "Failed"
	default:
		return strconv.Itoa(int(s))
	}
}

type OperationIntent int

const (
	OperationIntentProgress OperationIntent = 1 << iota
	OperationIntentObserve

	OperationIntentUnspecified = OperationIntent(0)
)

// The operation intent must come from the context
// as the handler may not pass the endpoint request as Input to,
// say, the chasm.UpdateComponent method.
// So similar to the chasm engine, handler needs to add the intent
// to the context.
type operationIntentCtxKeyType struct{}

var operationIntentCtxKey = operationIntentCtxKeyType{}

func newContextWithOperationIntent(
	ctx context.Context,
	intent OperationIntent,
) context.Context {
	return context.WithValue(ctx, operationIntentCtxKey, intent)
}

func operationIntentFromContext(
	ctx context.Context,
) OperationIntent {
	intent, ok := ctx.Value(operationIntentCtxKey).(OperationIntent)
	if !ok {
		return OperationIntentUnspecified
	}
	return intent
}
