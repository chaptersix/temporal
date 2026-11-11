package decision

// reconcileActivity derives the keyed summary from phase changes, without reading
// unrelated operations. Replacing the selected record during admission adds a new
// intent; it does not remove the previously selected intent.
func reconcileActivity(before, next State, event Event) State {
	if next.Operation != nil && (before.Operation == nil || before.Operation.ID != next.Operation.ID) {
		next.Root.UnconfirmedStarts += unconfirmed(next.Operation)
	} else {
		next.Root.UnconfirmedStarts += unconfirmed(next.Operation) - unconfirmed(before.Operation)
	}
	if next.Root.Revision > before.Root.Revision || next.Root.ManualSequence > before.Root.ManualSequence {
		advanceActivity(&next.Root, event.Now)
	}
	op := next.Operation
	if event.Kind == Acknowledged && event.StartObserved && op != nil && event.OperationID == op.ID && event.ChainID != "" && event.ChainID == op.ChainID && !op.StartObserved {
		op.StartObserved = true
		op.StartObservedAt = event.StartObservedAt
		advanceActivity(&next.Root, event.StartObservedAt)
	}
	return next
}

func unconfirmed(op *Operation) int64 {
	if op != nil && (op.Phase == Pending || op.Phase == Ambiguous) {
		return 1
	}
	return 0
}

func advanceActivity(root *Root, at int64) {
	if !root.ActivitySet || at > root.ActivityAt {
		root.ActivityAt = at
	}
	root.ActivitySet = true
}
