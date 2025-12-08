//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination library_mock.go

package chasm

import (
	"google.golang.org/grpc"
)

type (
	// Library represents a CHASM archetype implementation containing components, tasks, and API handlers.
	//
	// Libraries package related functionality for a specific archetype (e.g., Scheduler, Callback).
	// They define the component types, task executors, and gRPC service handlers that implement
	// the archetype's behavior.
	//
	// # Structure
	//
	// A typical library contains:
	//   - Root component type (the archetype, e.g., *Scheduler)
	//   - Child component types (e.g., *Generator, *Invoker)
	//   - Task executors (pure and side-effect)
	//   - gRPC service handlers for external API
	//   - Component and task registration metadata
	//
	// # Implementation
	//
	// Implement Library by embedding UnimplementedLibrary and defining:
	//
	//	type SchedulerLibrary struct {
	//	    chasm.UnimplementedLibrary
	//	}
	//
	//	func (l *SchedulerLibrary) Name() string {
	//	    return "scheduler"
	//	}
	//
	//	func (l *SchedulerLibrary) Components() []*chasm.RegistrableComponent {
	//	    return []*chasm.RegistrableComponent{
	//	        chasm.NewRegistrableComponent[*Scheduler]("scheduler"),
	//	        chasm.NewRegistrableComponent[*Generator]("generator"),
	//	    }
	//	}
	//
	//	func (l *SchedulerLibrary) Tasks() []*chasm.RegistrableTask {
	//	    return []*chasm.RegistrableTask{
	//	        chasm.NewRegistrablePureTask("generator", validator, executor),
	//	        chasm.NewRegistrableSideEffectTask("invoker", validator, executor),
	//	    }
	//	}
	//
	// # Registration
	//
	// Libraries are registered with the Registry at startup:
	//
	//	registry := chasm.NewRegistry(logger)
	//	registry.Register(&SchedulerLibrary{})
	//
	// See README.md#library-architecture for detailed documentation.
	Library interface {
		Name() string
		Components() []*RegistrableComponent
		Tasks() []*RegistrableTask
		RegisterServices(server *grpc.Server)

		mustEmbedUnimplementedLibrary()
	}

	// UnimplementedLibrary provides forward compatibility for Library implementations.
	//
	// Embed UnimplementedLibrary in your library implementation to ensure forward compatibility
	// if new methods are added to the Library interface.
	UnimplementedLibrary struct{}

	namer interface {
		Name() string
	}
)

func (UnimplementedLibrary) Components() []*RegistrableComponent {
	return nil
}

func (UnimplementedLibrary) Tasks() []*RegistrableTask {
	return nil
}

// RegisterServices Registers the gRPC calls to the handlers of the library.
func (UnimplementedLibrary) RegisterServices(_ *grpc.Server) {
}

func (UnimplementedLibrary) mustEmbedUnimplementedLibrary() {}

func fullyQualifiedName(libName, name string) string {
	return libName + "." + name
}
