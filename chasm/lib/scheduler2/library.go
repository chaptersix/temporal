package scheduler2

import "go.temporal.io/server/chasm"

type Library struct {
	chasm.UnimplementedLibrary
	start   *StartHandler
	effects *EffectHandler
}

// NewNilLibrary exposes independent identities to offline readers.
func NewNilLibrary() *Library { return &Library{} }

// NewLibrary selects an explicitly supplied local workflow starter.
func NewLibrary(start *StartHandler, effects *EffectHandler) *Library {
	return &Library{start: start, effects: effects}
}
func (*Library) Name() string { return "scheduler2" }
func (*Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler]("scheduler"),
		chasm.NewRegistrableComponent[*Generator]("generator"),
	}
}
func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{chasm.NewRegistrableSideEffectTask("start", l.start), chasm.NewRegistrableSideEffectTask("effect", l.effects), chasm.NewRegistrablePureTask("generate", &GenerateHandler{}), chasm.NewRegistrablePureTask("drain", &DrainHandler{}), chasm.NewRegistrablePureTask("manual", &ManualHandler{})}
}
