package chasm

import (
	"reflect"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

var (
	defaultShardingFn = func(key ExecutionKey) string { return key.NamespaceID + "_" + key.BusinessID }
)

// ExecutionKey uniquely identifies a CHASM execution in the system.
//
// Executions are identified by:
//   - NamespaceID: Temporal namespace containing the execution
//   - BusinessID: User-provided business identifier (stable across runs)
//   - RunID: Framework-generated unique identifier for a specific run
//
// The BusinessID is stable and can be reused across multiple runs (depending on
// BusinessIDReusePolicy). The RunID is unique per run and never reused.
type ExecutionKey struct {
	NamespaceID string
	BusinessID  string
	RunID       string
}

// ComponentRef is a reference to a component within a CHASM execution.
//
// ComponentRef identifies a specific component in the component tree and is used with Engine
// operations to target components for read, update, or create operations.
//
// # Creation
//
// Create a ComponentRef using NewComponentRef with the root component type:
//
//	ref := chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{
//	    NamespaceID: "default",
//	    BusinessID:  "schedule-123",
//	})
//
// The type parameter determines the archetype (root component type). The framework uses this
// to validate that operations target the correct component type.
//
// # Usage with Engine
//
// Pass ComponentRef to Engine operations:
//
//	// Create new execution
//	executionKey, ref, err := engine.NewExecution(ctx, ref, constructorFunc)
//
//	// Update component
//	ref, err := engine.UpdateComponent(ctx, ref, updateFunc)
//
//	// Read component
//	err := engine.ReadComponent(ctx, ref, readFunc)
//
// # Reference Semantics
//
// ComponentRef contains:
//   - ExecutionKey: Identifies the execution (NamespaceID, BusinessID, RunID)
//   - Component path: Location within the component tree (root vs child)
//   - Version information: For optimistic concurrency control
//   - Type information: For archetype validation
//
// In V1, ComponentRef always references the root component of an execution. Future versions
// may support referencing child components directly.
//
// See README.md#componentref for detailed documentation.
type ComponentRef struct {
	ExecutionKey

	// archetypeID is CHASM framework's internal ID for the type of the root component of the CHASM execution.
	//
	// It is used to find and validate the loaded execution has the right archetype, especially when runID
	// is not specified in the ExecutionKey.
	archetypeID ArchetypeID
	// executionGoType is used for determining the ComponetRef's archetype.
	// When CHASM deverloper needs to create a ComponentRef, they will only provide the component type,
	// and leave the work of determining archetypeID to the CHASM framework.
	executionGoType reflect.Type

	// executionLastUpdateVT is the consistency token for the entire execution.
	executionLastUpdateVT *persistencespb.VersionedTransition

	// componentType is the fully qualified component type name.
	// It is for performing partial loading more efficiently in future versions of CHASM.
	//
	// From the componentType, we can find the registered component struct definition,
	// then use reflection to find sub-components and understand if those sub-components
	// need to be loaded or not.
	// We only need to do this for sub-components, path for parent/ancenstor components
	// can be inferred from the current component path and they always needs to be loaded.
	//
	// componentType string

	// componentPath and componentInitialVT are used to identify a component.
	componentPath      []string
	componentInitialVT *persistencespb.VersionedTransition

	validationFn func(NodeBackend, Context, Component) error
}

// NewComponentRef creates a ComponentRef for a root component of type C.
//
// The type parameter C determines the archetype (root component type) for the execution.
// This is used by the framework to validate that Engine operations target the correct
// component type.
//
// Example:
//
//	ref := chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{
//	    NamespaceID: "default",
//	    BusinessID:  "schedule-daily-report",
//	})
//
// In V1, ComponentRef always references the root (top-level) component of an execution.
// To interact with child components, access them through the root component's fields.
func NewComponentRef[C Component](
	executionKey ExecutionKey,
) ComponentRef {
	return ComponentRef{
		ExecutionKey:    executionKey,
		executionGoType: reflect.TypeFor[C](),
	}
}

func (r *ComponentRef) ArchetypeID(
	registry *Registry,
) (ArchetypeID, error) {
	if r.archetypeID != UnspecifiedArchetypeID {
		return r.archetypeID, nil
	}

	rc, ok := registry.componentOf(r.executionGoType)
	if !ok {
		return 0, serviceerror.NewInternal("unknown chasm component type: " + r.executionGoType.String())
	}
	r.archetypeID = rc.componentID

	return r.archetypeID, nil
}

// ShardingKey returns the sharding key used for determining the shardID of the run
// that contains the referenced component.
// TODO: remove this method and ShardingKey concept, we don't need this functionality.
func (r *ComponentRef) ShardingKey(
	registry *Registry,
) (string, error) {

	archetypeID, err := r.ArchetypeID(registry)
	if err != nil {
		return "", err
	}

	rc, ok := registry.ComponentByID(archetypeID)
	if !ok {
		return "", serviceerror.NewInternalf("unknown chasm component type id: %d", archetypeID)
	}

	return rc.shardingFn(r.ExecutionKey), nil
}

func (r *ComponentRef) Serialize(
	registry *Registry,
) ([]byte, error) {
	if r == nil {
		return nil, nil
	}

	archetypeID, err := r.ArchetypeID(registry)
	if err != nil {
		return nil, err
	}

	pRef := persistencespb.ChasmComponentRef{
		NamespaceId:                         r.NamespaceID,
		BusinessId:                          r.BusinessID,
		RunId:                               r.RunID,
		ArchetypeId:                         archetypeID,
		ExecutionVersionedTransition:        r.executionLastUpdateVT,
		ComponentPath:                       r.componentPath,
		ComponentInitialVersionedTransition: r.componentInitialVT,
	}
	return pRef.Marshal()
}

// DeserializeComponentRef deserializes a byte slice into a ComponentRef.
// Provides caller the access to information including ExecutionKey, Archetype, and ShardingKey.
func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	var pRef persistencespb.ChasmComponentRef
	if err := pRef.Unmarshal(data); err != nil {
		return ComponentRef{}, err
	}

	return ProtoRefToComponentRef(&pRef), nil
}

// ProtoRefToComponentRef converts a persistence ChasmComponentRef reference to a
// ComponentRef. This is useful for situations where the protobuf ComponentRef has
// already been deserialized as part of an enclosing message.
func ProtoRefToComponentRef(pRef *persistencespb.ChasmComponentRef) ComponentRef {
	return ComponentRef{
		ExecutionKey: ExecutionKey{
			NamespaceID: pRef.NamespaceId,
			BusinessID:  pRef.BusinessId,
			RunID:       pRef.RunId,
		},
		archetypeID:           pRef.ArchetypeId,
		executionLastUpdateVT: pRef.ExecutionVersionedTransition,
		componentPath:         pRef.ComponentPath,
		componentInitialVT:    pRef.ComponentInitialVersionedTransition,
	}
}
