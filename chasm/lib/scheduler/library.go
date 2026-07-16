package scheduler

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/grpc"
)

type (
	Library struct {
		chasm.UnimplementedLibrary

		config  *Config
		handler *handler

		SchedulerIdleTaskHandler        *SchedulerIdleTaskHandler
		SchedulerCallbacksTaskHandler   *SchedulerCallbacksTaskHandler
		GeneratorTaskHandler            *GeneratorTaskHandler
		InvokerExecuteTaskHandler       *InvokerExecuteTaskHandler
		InvokerProcessBufferTaskHandler *InvokerProcessBufferTaskHandler
		BackfillerTaskHandler           *BackfillerTaskHandler
		MigrateToWorkflowTaskHandler    *SchedulerMigrateToWorkflowTaskHandler
	}
)

// NewNilLibrary creates a Library with all nil handlers. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() *Library {
	return &Library{}
}

func NewLibrary(
	config *Config,
	handler *handler,
	SchedulerIdleTaskHandler *SchedulerIdleTaskHandler,
	SchedulerCallbacksTaskHandler *SchedulerCallbacksTaskHandler,
	GeneratorTaskHandler *GeneratorTaskHandler,
	InvokerExecuteTaskHandler *InvokerExecuteTaskHandler,
	InvokerProcessBufferTaskHandler *InvokerProcessBufferTaskHandler,
	BackfillerTaskHandler *BackfillerTaskHandler,
	MigrateToWorkflowTaskHandler *SchedulerMigrateToWorkflowTaskHandler,
) *Library {
	return &Library{
		config:                          config,
		handler:                         handler,
		SchedulerIdleTaskHandler:        SchedulerIdleTaskHandler,
		SchedulerCallbacksTaskHandler:   SchedulerCallbacksTaskHandler,
		GeneratorTaskHandler:            GeneratorTaskHandler,
		InvokerExecuteTaskHandler:       InvokerExecuteTaskHandler,
		InvokerProcessBufferTaskHandler: InvokerProcessBufferTaskHandler,
		BackfillerTaskHandler:           BackfillerTaskHandler,
		MigrateToWorkflowTaskHandler:    MigrateToWorkflowTaskHandler,
	}
}

func (l *Library) Name() string {
	return chasm.SchedulerLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler](
			chasm.SchedulerComponentName,
			chasm.WithBusinessIDAlias("ScheduleId"),
			chasm.WithSearchAttributes(
				executionStatusSearchAttribute,
				scheduleNextActionTimeSearchAttribute,
				scheduleIdleCloseTimeSearchAttribute,
				scheduleRunningWorkflowCountSearchAttribute,
				scheduleBufferedStartsCountSearchAttribute,
			),
			// Exposes Tweakables to scheduler components via the CHASM context
			// (see tweakablesFromContext).
			chasm.WithContextValues(l.config.contextValues()),
		),
		chasm.NewRegistrableComponent[*Generator]("generator"),
		chasm.NewRegistrableComponent[*Invoker]("invoker"),
		chasm.NewRegistrableComponent[*Backfiller]("backfiller"),
		chasm.NewRegistrableComponent[*EventLog]("eventlog"),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"idle",
			l.SchedulerIdleTaskHandler,
		),
		chasm.NewRegistrableSideEffectTask(
			"callbacks",
			l.SchedulerCallbacksTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"generate",
			l.GeneratorTaskHandler,
		),
		chasm.NewRegistrableSideEffectTask(
			"execute",
			l.InvokerExecuteTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"processBuffer",
			l.InvokerProcessBufferTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"backfill",
			l.BackfillerTaskHandler,
			// LastProcessedTime is the backfill's schedule-time cursor, not a task-execution
			// watermark. For backfills over past ranges, a retry's wall-clock scheduled time
			// remains after that cursor, so high-water-mark validation alone cannot invalidate
			// the task that just ran. Replace it when scheduling the next retry to prevent
			// already-processed tasks from accumulating and executing again.
			chasm.WithSingletonTask(chasm.SingletonTaskModeReplace),
		),
		chasm.NewRegistrableSideEffectTask(
			"migrateToWorkflow",
			l.MigrateToWorkflowTaskHandler,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&schedulerpb.SchedulerService_ServiceDesc, l.handler)
}
