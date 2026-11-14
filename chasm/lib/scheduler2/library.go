package scheduler2

import (
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/dynamicconfig"
	legacy "go.temporal.io/server/service/worker/scheduler"
)

type Library struct {
	chasm.UnimplementedLibrary
	start         *StartHandler
	effects       *EffectHandler
	specBuilder   *legacy.SpecBuilder
	tweaks        func(string) schedulerconfig.Tweakables
	workQuanta    int
	bounded       bool
	preparation   *PreparationService
	previewLimits func() (int, int)
}

// NewNilLibrary exposes independent identities to offline readers.
func NewNilLibrary() *Library {
	config := dynamicconfig.NewNoopCollection()
	warn, maxIterations := dynamicconfig.SchedulerSpecWarnIterations.Get(config), dynamicconfig.SchedulerSpecMaxIterations.Get(config)
	return &Library{previewLimits: func() (int, int) { return warn(), maxIterations() }, tweaks: schedulerconfig.CurrentTweakables.Get(config), specBuilder: legacy.NewSpecBuilder(dynamicconfig.SchedulerSpecWarnIterations.Get(config), dynamicconfig.SchedulerSpecMaxIterations.Get(config))}
}

// NewLibrary selects an explicitly supplied local workflow starter.
func NewLibrary(start *StartHandler, effects *EffectHandler) *Library {
	library := NewNilLibrary()
	library.start, library.effects = start, effects
	library.workQuanta = coordinatorTransactionQuanta
	return library
}

// newBoundedNilLibrary registers only the private fresh addressed profile.
// Offline readers use the same layout as the live private library.
func newBoundedNilLibrary() *Library {
	l := NewNilLibrary()
	l.bounded = true
	return l
}

func newBoundedLibrary(start *StartHandler, effects *EffectHandler) *Library {
	l := newBoundedNilLibrary()
	l.start, l.effects = start, effects
	l.workQuanta = coordinatorTransactionQuanta
	return l
}

// addressedLayout limits follow evidence/wi018/bounded-scheduler-task-limits.md.
// Stale duplicate immediate attempts count against the 103 limit as well.
func addressedLayout() chasm.BoundedDataLayout {
	return chasm.BoundedDataLayout{MaxImmediateExecutions: 103, Components: []chasm.BoundedComponentSlot{
		chasm.BoundedComponent[*Scheduler](nil, chasm.BoundedTaskLimits{Pure: 3, SideEffect: 4, NewPerTransaction: 30, ImmediatePerTransaction: 103}).WithRequiredEmptyMaps("Operations", "Requests", "PayloadBlobs", "EffectRequests"),
		chasm.BoundedComponent[*Generator]([]string{"Generator"}, chasm.BoundedTaskLimits{}),
		chasm.BoundedComponent[*DefinitionPreparation]([]string{"Generator", "PreparedDefinition"}, chasm.BoundedTaskLimits{}),
		chasm.BoundedComponent[*TimezonePreparation]([]string{"Generator", "PreparedDefinition", "Timezone"}, chasm.BoundedTaskLimits{}),
		chasm.BoundedComponent[*chasm.Visibility]([]string{"Visibility"}, chasm.BoundedTaskLimits{SideEffect: 1, NewPerTransaction: 4}),
	}}
}

func (*Library) Name() string { return "scheduler2" }
func (l *Library) Components() []*chasm.RegistrableComponent {
	opts := []chasm.RegistrableComponentOption{chasm.WithBusinessIDAlias("ScheduleId"), chasm.WithSearchAttributes(publicNextActionTime, publicRunningWorkflowCount, publicBufferedStartsCount, publicIdleCloseTime), chasm.WithContextValues(map[any]any{publicSpecBuilderKey{}: l.specBuilder, previewLimitsKey{}: l.previewLimits, backfillBatchConfigKey{}: l.tweaks, workQuantaKey{}: l.workQuanta, publicListSpecLimitKey{}: func() int { return legacy.CurrentTweakablePolicies.SpecFieldLengthLimit }})}
	if l.bounded {
		opts = append(opts, chasm.WithBoundedDataLayout(addressedLayout()))
	}
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler]("scheduler", opts...),
		chasm.NewRegistrableComponent[*Generator]("generator"),
		chasm.NewRegistrableComponent[*CalendarPreparation]("calendarPreparation"),
		chasm.NewRegistrableComponent[*DefinitionPreparation]("definitionPreparation"),
		chasm.NewRegistrableComponent[*TimezonePreparation]("timezonePreparation"),
	}
}
func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{chasm.NewRegistrableSideEffectTask("sourcePreparation", &SourcePreparationHandler{service: l.preparation}), chasm.NewRegistrablePureTask("idle", &IdleHandler{}), chasm.NewRegistrablePureTask("work", &WorkHandler{}), chasm.NewRegistrableSideEffectTask("start", l.start), chasm.NewRegistrableSideEffectTask("startDispatch", &StartDispatchHandler{start: l.start}), chasm.NewRegistrablePureTask("startRecovery", &StartRecoveryHandler{}), chasm.NewRegistrableSideEffectTask("effect", l.effects), chasm.NewRegistrablePureTask("generate", &GenerateHandler{}), chasm.NewRegistrablePureTask("drain", &DrainHandler{}), chasm.NewRegistrablePureTask("manual", &ManualHandler{})}
}

type previewLimitsKey struct{}

func previewLimits(ctx chasm.Context) func() (int, int) {
	if limits, ok := ctx.Value(previewLimitsKey{}).(func() (int, int)); ok && limits != nil {
		return limits
	}
	return func() (int, int) { return 0, 0 }
}
