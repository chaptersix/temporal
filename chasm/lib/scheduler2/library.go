package scheduler2

import (
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/dynamicconfig"
	legacy "go.temporal.io/server/service/worker/scheduler"
)

type Library struct {
	chasm.UnimplementedLibrary
	start       *StartHandler
	effects     *EffectHandler
	specBuilder *legacy.SpecBuilder
	tweaks      func(string) schedulerconfig.Tweakables
	workQuanta  int
}

// NewNilLibrary exposes independent identities to offline readers.
func NewNilLibrary() *Library {
	config := dynamicconfig.NewNoopCollection()
	return &Library{tweaks: schedulerconfig.CurrentTweakables.Get(config), specBuilder: legacy.NewSpecBuilder(dynamicconfig.SchedulerSpecWarnIterations.Get(config), dynamicconfig.SchedulerSpecMaxIterations.Get(config))}
}

// NewLibrary selects an explicitly supplied local workflow starter.
func NewLibrary(start *StartHandler, effects *EffectHandler) *Library {
	library := NewNilLibrary()
	library.start, library.effects = start, effects
	library.workQuanta = coordinatorTransactionQuanta
	return library
}
func (*Library) Name() string { return "scheduler2" }
func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler]("scheduler", chasm.WithBusinessIDAlias("ScheduleId"), chasm.WithSearchAttributes(publicNextActionTime, publicRunningWorkflowCount, publicBufferedStartsCount, publicIdleCloseTime), chasm.WithContextValues(map[any]any{publicSpecBuilderKey{}: l.specBuilder, backfillBatchConfigKey{}: l.tweaks, workQuantaKey{}: l.workQuanta, publicListSpecLimitKey{}: func() int { return legacy.CurrentTweakablePolicies.SpecFieldLengthLimit }})),
		chasm.NewRegistrableComponent[*Generator]("generator"),
		chasm.NewRegistrableComponent[*CalendarPreparation]("calendarPreparation"),
	}
}
func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{chasm.NewRegistrablePureTask("idle", &IdleHandler{}), chasm.NewRegistrablePureTask("work", &WorkHandler{}), chasm.NewRegistrableSideEffectTask("start", l.start), chasm.NewRegistrableSideEffectTask("effect", l.effects), chasm.NewRegistrablePureTask("generate", &GenerateHandler{}), chasm.NewRegistrablePureTask("drain", &DrainHandler{}), chasm.NewRegistrablePureTask("manual", &ManualHandler{})}
}
