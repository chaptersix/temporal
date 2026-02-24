package tasks

//go:generate go tool mockgen -package $GOPACKAGE -source $GOFILE -destination scheduler_mock.go
type (
	// Scheduler is the generic interface for scheduling & processing tasks
	Scheduler[T Task] interface {
		Submit(task T)
		TrySubmit(task T) bool
		Start()
		Stop()
	}
)
