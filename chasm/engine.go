//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination engine_mock.go

package chasm

import (
	"context"
)

// NoValue is a sentinel type representing no value.
// Useful for accessing components using the engine methods (e.g., [GetComponent]) with a function that does not need to
// return any information.
type NoValue = *struct{}

// Engine is the primary API for interacting with CHASM executions.
// It provides operations for creating new executions (NewExecution),
// updating existing components (UpdateComponent), and reading component
// state (ReadComponent). All write operations are transactional with
// optimistic concurrency control.
//
// Write operations create a transaction that commits atomically. If the
// transition function returns an error, the transaction is rolled back.
// Read operations provide a snapshot view without creating a transaction.
type Engine interface {
	NewExecution(
		context.Context,
		ComponentRef,
		func(MutableContext) (Component, error),
		...TransitionOption,
	) (ExecutionKey, []byte, error)
	UpdateWithNewExecution(
		context.Context,
		ComponentRef,
		func(MutableContext) (Component, error),
		func(MutableContext, Component) error,
		...TransitionOption,
	) (ExecutionKey, []byte, error)

	UpdateComponent(
		context.Context,
		ComponentRef,
		func(MutableContext, Component) error,
		...TransitionOption,
	) ([]byte, error)
	ReadComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) error,
		...TransitionOption,
	) error

	PollComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) (any, bool, error),
		func(MutableContext, Component, any) error,
		...TransitionOption,
	) ([]byte, error)
}

// BusinessIDReusePolicy controls whether duplicate BusinessIDs are allowed when creating executions.
type BusinessIDReusePolicy int

const (
	// BusinessIDReusePolicyAllowDuplicate allows creating a new execution even if one with the
	// same BusinessID already exists (regardless of state).
	BusinessIDReusePolicyAllowDuplicate BusinessIDReusePolicy = iota

	// BusinessIDReusePolicyAllowDuplicateFailedOnly allows creating a new execution only if
	// the existing execution with the same BusinessID is in a failed state.
	BusinessIDReusePolicyAllowDuplicateFailedOnly

	// BusinessIDReusePolicyRejectDuplicate rejects creating a new execution if one with the
	// same BusinessID already exists.
	BusinessIDReusePolicyRejectDuplicate
)

// BusinessIDConflictPolicy determines behavior when a BusinessID conflict is detected.
type BusinessIDConflictPolicy int

const (
	// BusinessIDConflictPolicyFail returns an error when a conflict is detected.
	BusinessIDConflictPolicyFail BusinessIDConflictPolicy = iota

	// BusinessIDConflictPolicyTerminateExisting terminates the existing execution and
	// creates a new one with the same BusinessID.
	BusinessIDConflictPolicyTerminateExisting

	// BusinessIDConflictPolicyUseExisting uses the existing execution instead of creating
	// a new one (effectively becomes an update operation).
	BusinessIDConflictPolicyUseExisting
)

// TransitionOptions configures Engine operations like NewExecution and UpdateComponent.
//
// Options control execution creation behavior, request idempotency, and transaction persistence.
type TransitionOptions struct {
	ReusePolicy    BusinessIDReusePolicy
	ConflictPolicy BusinessIDConflictPolicy
	RequestID      string
	Speculative    bool
}

// TransitionOption is a functional option for configuring Engine operations.
type TransitionOption func(*TransitionOptions)

// WithSpeculative marks a transition as speculative (not immediately persisted).
//
// Speculative transitions are buffered and persisted together with the next non-speculative
// transition. This is useful for batching multiple state changes into a single persistence
// operation for efficiency.
//
// Scope: This option applies to a single transition. Compare with ExecutionEphemeral() on
// RegistrableComponent, which applies to all transitions for a component type.
//
// Example:
//
//	// First transition is speculative (buffered)
//	engine.UpdateComponent(ctx, ref, updateFunc, chasm.WithSpeculative())
//
//	// Second transition persists both changes atomically
//	engine.UpdateComponent(ctx, ref, anotherUpdateFunc)
//
// Note: Task scheduling in speculative transitions requires special handling (see implementation).
func WithSpeculative() TransitionOption {
	return func(opts *TransitionOptions) {
		opts.Speculative = true
	}
}

// WithBusinessIDPolicy configures BusinessID reuse and conflict behavior for execution creation.
//
// This option controls what happens when attempting to create an execution with a BusinessID
// that already exists. It only applies to NewExecution() and UpdateWithNewExecution().
//
// ReusePolicy options:
//   - AllowDuplicate: Allow new execution regardless of existing state
//   - AllowDuplicateFailedOnly: Allow new execution only if existing one failed
//   - RejectDuplicate: Fail if BusinessID already exists
//
// ConflictPolicy options (when duplicate is detected):
//   - Fail: Return an error
//   - TerminateExisting: Terminate existing execution and create new one
//   - UseExisting: Use existing execution (becomes an update)
//
// Example:
//
//	engine.NewExecution(
//	    ctx,
//	    ref,
//	    constructorFunc,
//	    chasm.WithBusinessIDPolicy(
//	        chasm.BusinessIDReusePolicyRejectDuplicate,
//	        chasm.BusinessIDConflictPolicyFail,
//	    ),
//	)
func WithBusinessIDPolicy(
	reusePolicy BusinessIDReusePolicy,
	conflictPolicy BusinessIDConflictPolicy,
) TransitionOption {
	return func(opts *TransitionOptions) {
		opts.ReusePolicy = reusePolicy
		opts.ConflictPolicy = conflictPolicy
	}
}

// WithRequestID sets a request ID for idempotent execution creation.
//
// When provided, the request ID is used to deduplicate execution creation requests. Multiple
// requests with the same BusinessID and RequestID will create only one execution, making the
// operation idempotent.
//
// This option only applies to NewExecution() and UpdateWithNewExecution().
//
// Example:
//
//	engine.NewExecution(
//	    ctx,
//	    ref,
//	    constructorFunc,
//	    chasm.WithRequestID(uuid.New().String()),
//	)
func WithRequestID(
	requestID string,
) TransitionOption {
	return func(opts *TransitionOptions) {
		opts.RequestID = requestID
	}
}

// Not needed for V1
// func WithEagerLoading(
// 	paths []ComponentPath,
// ) OperationOption {
// 	panic("not implemented")
// }

func NewExecution[C Component, I any, O any](
	ctx context.Context,
	key ExecutionKey,
	newFn func(MutableContext, I) (C, O, error),
	input I,
	opts ...TransitionOption,
) (O, ExecutionKey, []byte, error) {
	var output O
	executionKey, serializedRef, err := engineFromContext(ctx).NewExecution(
		ctx,
		NewComponentRef[C](key),
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output, err = newFn(ctx, input)
			return c, err
		},
		opts...,
	)
	if err != nil {
		return output, ExecutionKey{}, nil, err
	}
	return output, executionKey, serializedRef, err
}

func UpdateWithNewExecution[C Component, I any, O1 any, O2 any](
	ctx context.Context,
	key ExecutionKey,
	newFn func(MutableContext, I) (C, O1, error),
	updateFn func(C, MutableContext, I) (O2, error),
	input I,
	opts ...TransitionOption,
) (O1, O2, ExecutionKey, []byte, error) {
	var output1 O1
	var output2 O2
	executionKey, serializedRef, err := engineFromContext(ctx).UpdateWithNewExecution(
		ctx,
		NewComponentRef[C](key),
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output1, err = newFn(ctx, input)
			return c, err
		},
		func(ctx MutableContext, c Component) error {
			var err error
			output2, err = updateFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)
	if err != nil {
		return output1, output2, ExecutionKey{}, nil, err
	}
	return output1, output2, executionKey, serializedRef, err
}

// TODO:
//   - consider merge with ReadComponent
//   - consider remove ComponentRef from the return value and allow components to get
//     the ref in the transition function. There are some caveats there, check the
//     comment of the NewRef method in MutableContext.
func UpdateComponent[C any, R []byte | ComponentRef, I any, O any](
	ctx context.Context,
	r R,
	updateFn func(C, MutableContext, I) (O, error),
	input I,
	opts ...TransitionOption,
) (O, []byte, error) {
	var output O

	ref, err := convertComponentRef(r)
	if err != nil {
		return output, nil, err
	}

	newSerializedRef, err := engineFromContext(ctx).UpdateComponent(
		ctx,
		ref,
		func(ctx MutableContext, c Component) error {
			var err error
			output, err = updateFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)

	if err != nil {
		return output, nil, err
	}
	return output, newSerializedRef, err
}

func ReadComponent[C any, R []byte | ComponentRef, I any, O any](
	ctx context.Context,
	r R,
	readFn func(C, Context, I) (O, error),
	input I,
	opts ...TransitionOption,
) (O, error) {
	var output O

	ref, err := convertComponentRef(r)
	if err != nil {
		return output, err
	}

	err = engineFromContext(ctx).ReadComponent(
		ctx,
		ref,
		func(ctx Context, c Component) error {
			var err error
			output, err = readFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)
	return output, err
}

func PollComponent[C any, R []byte | ComponentRef, I any, O any, T any](
	ctx context.Context,
	r R,
	predicateFn func(C, Context, I) (T, bool, error),
	operationFn func(C, MutableContext, I, T) (O, error),
	input I,
	opts ...TransitionOption,
) (O, []byte, error) {
	var output O

	ref, err := convertComponentRef(r)
	if err != nil {
		return output, nil, err
	}

	newSerializedRef, err := engineFromContext(ctx).PollComponent(
		ctx,
		ref,
		func(ctx Context, c Component) (any, bool, error) {
			return predicateFn(c.(C), ctx, input)
		},
		func(ctx MutableContext, c Component, t any) error {
			var err error
			output, err = operationFn(c.(C), ctx, input, t.(T))
			return err
		},
		opts...,
	)
	if err != nil {
		return output, nil, err
	}
	return output, newSerializedRef, err
}

func convertComponentRef[R []byte | ComponentRef](
	r R,
) (ComponentRef, error) {
	if refToken, ok := any(r).([]byte); ok {
		return DeserializeComponentRef(refToken)
	}

	//revive:disable-next-line:unchecked-type-assertion
	return any(r).(ComponentRef), nil
}

type engineCtxKeyType string

const engineCtxKey engineCtxKeyType = "chasmEngine"

// this will be done by the nexus handler?
// alternatively the engine can be a global variable,
// but not a good practice in fx.
func NewEngineContext(
	ctx context.Context,
	engine Engine,
) context.Context {
	return context.WithValue(ctx, engineCtxKey, engine)
}

func engineFromContext(
	ctx context.Context,
) Engine {
	e, ok := ctx.Value(engineCtxKey).(Engine)
	if !ok {
		return nil
	}
	return e
}
