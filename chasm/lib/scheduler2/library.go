package scheduler2

import "go.temporal.io/server/chasm"

type Library struct {
	chasm.UnimplementedLibrary
	start *StartHandler
}

// NewNilLibrary exposes independent identities to offline readers.
func NewNilLibrary() *Library { return &Library{} }

// NewLibrary selects an explicitly supplied local workflow starter.
func NewLibrary(start *StartHandler) *Library { return &Library{start: start} }
func (*Library) Name() string                 { return "scheduler2" }
func (*Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Scheduler]("scheduler"),
		chasm.NewRegistrableComponent[*Operation]("operation"),
	}
}
func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{chasm.NewRegistrableSideEffectTask("start", l.start)}
}
