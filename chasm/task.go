//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_mock.go

package chasm

import (
	"context"
	"time"
)

type (
	// TaskAttributes controls task execution scheduling and routing.
	//
	// ScheduledTime: When the task should execute. Use time.Time{} or TaskScheduledTimeImmediate
	// for immediate execution. Future times schedule delayed execution.
	//
	// Destination: For side-effect tasks, specifies the target endpoint for the external operation
	// (e.g., HTTP endpoint for callbacks). Must be empty for pure tasks.
	//
	// Example immediate task:
	//
	//	ctx.AddTask(scheduler, chasm.TaskAttributes{}, &taskpb.GeneratorTask{})
	//
	// Example delayed task:
	//
	//	ctx.AddTask(
	//	    callback,
	//	    chasm.TaskAttributes{
	//	        ScheduledTime: ctx.Now(callback).Add(time.Hour),
	//	    },
	//	    &taskpb.BackoffTask{Attempt: 1},
	//	)
	//
	// Example side-effect task with destination:
	//
	//	ctx.AddTask(
	//	    callback,
	//	    chasm.TaskAttributes{
	//	        Destination: "https://api.example.com/callback",
	//	    },
	//	    &taskpb.InvocationTask{},
	//	)
	TaskAttributes struct {
		ScheduledTime time.Time
		Destination   string
	}

	// SideEffectTaskExecutor executes tasks that perform external operations (I/O, network calls).
	//
	// Side-effect tasks execute outside the CHASM transaction and can perform non-transactional
	// operations like HTTP calls, database queries, or file I/O. They receive a ComponentRef to
	// update state after completing the side effect.
	//
	// Example:
	//
	//	type InvocationTaskExecutor struct {
	//	    httpClient *http.Client
	//	}
	//
	//	func (e *InvocationTaskExecutor) Execute(
	//	    ctx context.Context,
	//	    ref chasm.ComponentRef,
	//	    attrs chasm.TaskAttributes,
	//	    task *taskpb.InvocationTask,
	//	) error {
	//	    // Perform side effect (HTTP call)
	//	    resp, err := e.httpClient.Post(attrs.Destination, task.Payload)
	//	    if err != nil {
	//	        return err
	//	    }
	//	    // Update component state with result
	//	    return engine.UpdateComponent(ctx, ref, func(ctx chasm.MutableContext, c chasm.Component) error {
	//	        return c.(*Callback).HandleResponse(ctx, resp)
	//	    })
	//	}
	//
	// See README.md#side-effect-tasks for detailed documentation.
	SideEffectTaskExecutor[C any, T any] interface {
		Execute(context.Context, ComponentRef, TaskAttributes, T) error
	}

	// PureTaskExecutor executes tasks within a CHASM transaction (no external I/O).
	//
	// Pure tasks execute within a transaction and can only modify component state through
	// MutableContext. They must be deterministic and idempotent. All state changes are
	// committed atomically.
	//
	// Example:
	//
	//	type GeneratorTaskExecutor struct{}
	//
	//	func (e *GeneratorTaskExecutor) Execute(
	//	    ctx chasm.MutableContext,
	//	    scheduler *Scheduler,
	//	    attrs chasm.TaskAttributes,
	//	    task *taskpb.GeneratorTask,
	//	) error {
	//	    // Read component state
	//	    spec, _ := scheduler.Spec.TryGet(ctx)
	//
	//	    // Generate scheduled actions
	//	    actions := generateActions(spec, ctx.Now(scheduler))
	//
	//	    // Update state and schedule follow-up tasks
	//	    scheduler.Actions = chasm.NewDataField(ctx, actions)
	//	    ctx.AddTask(scheduler, chasm.TaskAttributes{}, &taskpb.InvokerTask{})
	//	    return nil
	//	}
	//
	// See README.md#pure-tasks for detailed documentation.
	PureTaskExecutor[C any, T any] interface {
		Execute(MutableContext, C, TaskAttributes, T) error
	}

	// TaskValidator validates whether a task should execute.
	//
	// Validators are invoked before task execution to check preconditions. If validation
	// returns false or an error, the task is not executed.
	//
	// Example:
	//
	//	type GeneratorTaskValidator struct{}
	//
	//	func (v *GeneratorTaskValidator) Validate(
	//	    ctx chasm.Context,
	//	    scheduler *Scheduler,
	//	    attrs chasm.TaskAttributes,
	//	    task *taskpb.GeneratorTask,
	//	) (bool, error) {
	//	    // Only execute if scheduler is running
	//	    return scheduler.LifecycleState(ctx) == chasm.LifecycleStateRunning, nil
	//	}
	TaskValidator[C any, T any] interface {
		// Validate determines whether a task should proceed with execution based on the current context, component
		// state, task attributes, and task data.
		//
		// This function serves as a gate to prevent unnecessary task execution in several scenarios:
		// 1. Standby cluster deduplication: When state is replicated to standby clusters, tasks are also replicated.
		//    Validate allows standby clusters to check if a task was already completed on the active cluster and
		//    skip execution if so (e.g., checking if an activity already transitioned from scheduled to started state).
		// 2. Task obsolescence: Tasks can become irrelevant when state changes invalidate them (e.g., when a scheduler
		//    is updated to run at a different time, making the previously scheduled task invalid for the new state).
		//    For pure tasks that can run in a single transaction, Validate is called before execution to avoid
		//    unnecessary work.
		//
		// The framework automatically calls Validate at key points, such as after closing transactions, to check all
		// generated tasks before they execute.
		//
		// Returns:
		// - (true, nil) if the task is valid and should be executed
		// - (false, nil) if the task should be silently dropped (it's no longer relevant)
		// - (anything, error) if validation fails with an error
		Validate(Context, C, TaskAttributes, T) (bool, error)
	}
)

// TaskScheduledTimeImmediate is the zero value for immediate task execution.
var TaskScheduledTimeImmediate = time.Time{}

// IsImmediate returns true if the task should execute immediately (not delayed).
func (a *TaskAttributes) IsImmediate() bool {
	return a.ScheduledTime.IsZero() ||
		a.ScheduledTime.Equal(TaskScheduledTimeImmediate)
}

// IsValid returns true if the TaskAttributes are valid.
//
// Invalid combinations:
//   - Destination set with non-immediate ScheduledTime (routing not supported for delayed tasks)
func (a *TaskAttributes) IsValid() bool {
	return a.Destination == "" || a.IsImmediate()
}
