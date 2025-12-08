package chasm

import (
	"reflect"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
)

const (
	// Used by reflection.
	internalFieldName = "Internal"
)

// Field[T] is a type-safe container for component state with lazy deserialization.
// Fields can hold either data (protobuf messages via NewDataField) or child components
// (via NewComponentField). The framework handles serialization, deserialization, and
// persistence automatically.
//
// Fields are accessed via TryGet or Get and are lazily loaded from persistence only when
// accessed, minimizing memory footprint for large component trees. Fields are immutable once
// created; to update a field's value, create a new field with the updated value.
type Field[T any] struct {
	// This struct needs to be created via reflection, but reflection can't set private fields.
	Internal fieldInternal
}

// NewDataField creates a Field[D] containing a protobuf message.
// Data fields store protobuf messages as component state and are serialized/deserialized
// automatically. Use for configuration, results, and simple state without behavior.
func NewDataField[D proto.Message](
	ctx MutableContext,
	d D,
) Field[D] {
	return Field[D]{
		Internal: newFieldInternalWithValue(fieldTypeData, d),
	}
}

// NewComponentField creates a Field[C] containing a child component.
// Child components have their own state, lifecycle, and behavior, and are useful for
// decomposing complex archetypes into smaller pieces. Component fields support lazy
// loading - child components are deserialized only when accessed.
func NewComponentField[C Component](
	ctx MutableContext,
	c C,
	options ...ComponentFieldOption,
) Field[C] {
	return Field[C]{
		Internal: newFieldInternalWithValue(fieldTypeComponent, c),
	}
}

// ComponentPointerTo returns a CHASM field populated with a pointer to the given
// component. Pointers are resolved at the time the transaction is closed, and the
// transaction will fail if any pointers cannot be resolved.
func ComponentPointerTo[C Component](
	ctx MutableContext,
	c C,
) Field[C] {
	return Field[C]{
		Internal: newFieldInternalWithValue(fieldTypeDeferredPointer, c),
	}
}

// DataPointerTo returns a CHASM field populated with a pointer to the given
// message. Pointers are resolved at the time the transaction is closed, and the
// transaction will fail if any pointers cannot be resolved.
func DataPointerTo[D proto.Message](
	ctx MutableContext,
	d D,
) Field[D] {
	return Field[D]{
		Internal: newFieldInternalWithValue(fieldTypeDeferredPointer, d),
	}
}

// TryGet returns the value of the field and a boolean indicating if the value was found, deserializing if necessary.
// Panics rather than returning an error, as errors are supposed to be handled by the framework as opposed to the
// application, even if the error is an application bug.
func (f Field[T]) TryGet(chasmContext Context) (T, bool) {
	var nilT T

	// If node is nil, then there is nothing to deserialize from, return value (even if it is also nil).
	if f.Internal.node == nil {
		if f.Internal.v == nil {
			return nilT, false
		}
		vT, isT := f.Internal.v.(T)
		if !isT {
			// nolint:forbidigo // Panic is intended here for framework error handling.
			panic(serviceerror.NewInternalf("internal value doesn't implement %s", reflect.TypeFor[T]().Name()))
		}
		return vT, true
	}

	var nodeValue any
	switch f.Internal.fieldType() {
	case fieldTypeComponent:
		if err := f.Internal.node.prepareComponentValue(chasmContext); err != nil {
			// nolint:forbidigo // Panic is intended here for framework error handling.
			panic(err)
		}
		nodeValue = f.Internal.node.value
	case fieldTypeData:
		// For data fields, T is always a concrete type.
		if err := f.Internal.node.prepareDataValue(chasmContext, reflect.TypeFor[T]()); err != nil {
			// nolint:forbidigo // Panic is intended here for framework error handling.
			panic(err)
		}
		nodeValue = f.Internal.node.value
	case fieldTypePointer:
		if err := f.Internal.node.preparePointerValue(); err != nil {
			// nolint:forbidigo // Panic is intended here for framework error handling.
			panic(err)
		}
		//nolint:revive // value is guaranteed to be of type []string.
		path := f.Internal.value().([]string)
		if referencedNode, found := f.Internal.node.root().findNode(path); found {
			var err error
			switch referencedNode.fieldType() {
			case fieldTypeComponent:
				err = referencedNode.prepareComponentValue(chasmContext)
			case fieldTypeData:
				err = referencedNode.prepareDataValue(chasmContext, reflect.TypeFor[T]())
			default:
				err = serviceerror.NewInternalf("pointer field referenced an unhandled value: %v", referencedNode.fieldType())
			}
			if err != nil {
				// nolint:forbidigo // Panic is intended here for framework error handling.
				panic(err)
			}
			nodeValue = referencedNode.value
		}
	case fieldTypeDeferredPointer:
		// For deferred pointers, return the component directly stored in v
		nodeValue = f.Internal.v
	default:
		// nolint:forbidigo // Panic is intended here for framework error handling.
		panic(serviceerror.NewInternalf("unsupported field type: %v", f.Internal.fieldType()))
	}

	if nodeValue == nil {
		return nilT, false
	}
	vT, isT := nodeValue.(T)
	if !isT {
		// nolint:forbidigo // Panic is intended here for framework error handling.
		panic(serviceerror.NewInternalf("node value doesn't implement %s", reflect.TypeFor[T]().Name()))
	}
	return vT, true
}

// Get returns the value of the field, deserializing it if necessary.
// Panics rather than returning an error, as errors are supposed to be handled by the framework as opposed to the
// application, even if the error is an application bug.
func (f Field[T]) Get(chasmContext Context) T {
	v, ok := f.TryGet(chasmContext)
	if !ok {
		// nolint:forbidigo // Panic is intended here for framework error handling.
		panic(serviceerror.NewInternalf("field value of type %s not found", reflect.TypeFor[T]().Name()))
	}
	return v
}

func NewEmptyField[T any]() Field[T] {
	return Field[T]{}
}
