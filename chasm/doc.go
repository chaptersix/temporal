// Package chasm provides a framework for building durable, stateful components within Temporal Server.
//
// CHASM (Coordinated Heterogeneous Application State Machines) enables developers to implement
// complex stateful systems as composable components organized in a tree structure. The framework
// provides automatic persistence, lazy deserialization, type safety, task orchestration, and
// multi-cluster replication support.
//
// Components are stateful entities with behavior and lifecycle, organized in a tree where each
// component can have child components and data fields. The Engine provides the primary API for
// creating executions (NewExecution), updating components (UpdateComponent), and reading state
// (ReadComponent). Context provides read-only access to state, while MutableContext extends it
// with write capabilities for state mutations and task scheduling. Field[T] is a type-safe
// container for component state supporting lazy deserialization.
//
// Typical usage involves defining a component type, implementing the Component interface,
// and using the Engine to create and update component instances:
//
//	// Define a component
//	type Scheduler struct {
//	    chasm.UnimplementedComponent
//	    Spec   chasm.Field[*schedulerpb.ScheduleSpec]
//	    Paused chasm.Field[bool]
//	}
//
//	// Create a new execution
//	executionKey, ref, err := engine.NewExecution(
//	    ctx,
//	    componentRef,
//	    func(ctx chasm.MutableContext) (chasm.Component, error) {
//	        return &Scheduler{
//	            Spec:   chasm.NewDataField(ctx, spec),
//	            Paused: chasm.NewDataField(ctx, false),
//	        }, nil
//	    },
//	)
//
//	// Update the component
//	ref, err = engine.UpdateComponent(
//	    ctx,
//	    componentRef,
//	    func(ctx chasm.MutableContext, c chasm.Component) error {
//	        scheduler := c.(*Scheduler)
//	        paused, _ := scheduler.Paused.TryGet(ctx)
//	        if !paused {
//	            ctx.AddTask(scheduler, chasm.TaskAttributes{}, &taskpb.GeneratorTask{})
//	        }
//	        return nil
//	    },
//	)
//
// CHASM is designed for implementing archetypes within Temporal Server, including Scheduler
// (schedule-based workflow execution), Callback (asynchronous callback handling for Nexus
// operations), and Workflow (work in progress). Each archetype is implemented as a CHASM
// library containing related components, tasks, and API handlers.
//
// See the README.md file in the chasm directory for detailed documentation on component
// architecture, state machines, task system, and library registration. See lib/scheduler/README.md
// and lib/callback/README.md for archetype implementation examples.
package chasm
