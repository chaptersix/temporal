package decision

// Capacity is an execution input, not persisted policy. The zero value preserves
// the four-slot finite model; public callers supply the live namespace limit.
type Capacity struct {
	Public             bool
	MaxBuffer          int64
	BufferedOperations int64
}

// QueueAvailable means a new retained intent can acquire a vacancy. Existing
// queued intent can move to an operation even when this returns false.
func QueueAvailable(state State) bool {
	if state.Capacity.Public {
		return state.Capacity.MaxBuffer <= 0 || (state.Capacity.BufferedOperations < state.Capacity.MaxBuffer && QueueLength(state) < state.Capacity.MaxBuffer-state.Capacity.BufferedOperations)
	}
	return QueueLength(state) < 4
}

// OperationAvailable separates operation authority from public ingress limits.
// Public active operations are never evicted to satisfy a prototype slot bound.
func OperationAvailable(state State) bool {
	return state.Capacity.Public || state.Root.Unfinished < 4
}

func bufferedOperation(op *Operation) int64 {
	if op != nil && op.Phase != StartOnly {
		return 1
	}
	return 0
}
