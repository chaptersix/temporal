package schedulertest

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Violation describes a single broken invariant.
type Violation struct {
	Name    string
	Message string
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] %s", v.Name, v.Message)
}

// violations accumulates the broken invariants found by a check.
type violations []Violation

func (vs *violations) add(name, format string, args ...any) {
	*vs = append(*vs, Violation{Name: name, Message: fmt.Sprintf(format, args...)})
}

// CheckInvariants evaluates the scheduler invariants against a settled state
// (cur). When prev is non-nil it also checks cross-step invariants (monotonicity,
// closed-is-terminal). It returns all violations found; an empty slice means the
// state is healthy.
//
// The invariants encode the legitimate-quiescence rules from Scheduler.isHeldOpen
// and Scheduler.getIdleExpiration: a non-closed schedule may have no pending task
// only when it is held open (paused or draining a backfill). Any other taskless,
// non-closed state is the "stuck" bug class.
func CheckInvariants(prev *Snapshot, cur Snapshot) []Violation {
	var vs violations

	// (1) No-stuck (primary): a live schedule must always have a way to make
	// progress. The only taskless, non-closed state allowed is "held open"
	// (paused or pending backfill). When idle is armed there is a pending idle
	// task, so HasPendingTask covers that case.
	if !cur.Closed && !cur.HasPendingTask && !cur.IsHeldOpen {
		vs.add("no-stuck",
			"scheduler has no pending task, is not closed, and is not held open "+
				"(paused=%v backfillers=%d) at now=%s — stuck",
			cur.Paused, cur.BackfillerCount, cur.Now.Format("2006-01-02T15:04:05"))
	}

	// (2) Idle correctness: a held-open schedule must never have an idle-close
	// armed; idle close would race the customer's intent to resume/drain.
	if cur.IsHeldOpen && !cur.IdleCloseTime.IsZero() {
		vs.add("idle-not-while-held-open",
			"scheduler is held open (paused=%v backfillers=%d) but has idle close armed at %s",
			cur.Paused, cur.BackfillerCount, cur.IdleCloseTime.Format("2006-01-02T15:04:05"))
	}

	// (2a) Buffer bound: the total buffered starts must never exceed MaxBufferSize.
	// The generator sizes each fill against this cap (remaining = MaxBufferSize -
	// len(buffered)); exceeding it means the limit was not enforced and CHASM state
	// can grow without bound (oversized state, persistence failures, OOM).
	if cur.MaxBufferSize > 0 && cur.BufferedStartsCount > cur.MaxBufferSize {
		vs.add("buffer-bound",
			"buffered starts = %d, exceeding MaxBufferSize=%d",
			cur.BufferedStartsCount, cur.MaxBufferSize)
	}

	// (2b) Action budget: a single InvokerExecuteTask must not take more actions
	// (start + terminate + cancel RPCs) than MaxActionsPerExecution. Exceeding it
	// means the per-execution rate protection was bypassed — e.g. the action budget
	// reset across the terminate/cancel/start phases, letting one execution take up
	// to a multiple of the configured maximum.
	if cur.MaxActionsPerExecution > 0 && cur.MaxActionsObserved > cur.MaxActionsPerExecution {
		vs.add("action-budget",
			"a single ExecuteTask took %d actions, exceeding MaxActionsPerExecution=%d",
			cur.MaxActionsObserved, cur.MaxActionsPerExecution)
	}

	vs = append(vs, checkLedger(prev, cur)...)
	vs = append(vs, checkStartAccounting(cur)...)
	if !cur.Closed && !cur.IdleCloseTime.IsZero() && !cur.HasValidIdleTask {
		vs.add("idle-route", "advertised idle deadline %s has no valid logical idle task", cur.IdleCloseTime)
	}

	if prev != nil {
		// (3) Closed is terminal: a closed schedule must never reopen.
		if prev.Closed && !cur.Closed {
			vs.add("closed-terminal", "scheduler reopened after being closed")
		}

		// (4) High-water-mark monotonicity: generator/invoker HWMs never regress.
		if cur.GeneratorLPT.Before(prev.GeneratorLPT) {
			vs.add("hwm-monotonic-generator",
				"generator high-water mark went backward: %s -> %s",
				prev.GeneratorLPT.Format("2006-01-02T15:04:05"),
				cur.GeneratorLPT.Format("2006-01-02T15:04:05"))
		}
		if cur.InvokerLPT.Before(prev.InvokerLPT) {
			vs.add("hwm-monotonic-invoker",
				"invoker high-water mark went backward: %s -> %s",
				prev.InvokerLPT.Format("2006-01-02T15:04:05"),
				cur.InvokerLPT.Format("2006-01-02T15:04:05"))
		}
	}

	return vs
}

// fataler is the subset of testing.TB the invariant assertion helper needs.
type fataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

// CheckInvariantsHook returns an AfterStep hook that captures a snapshot after
// every step and fails the test, reporting every invariant violation found. It threads the
// previous snapshot so cross-step invariants are checked. This is the normal way
// invariant checking is wired into a Driver:
//
//	d := NewDriver(t)
//	d.AfterStep = CheckInvariantsHook(t)
func CheckInvariantsHook(t fataler) func(d *Driver) {
	var prev *Snapshot
	return func(d *Driver) {
		t.Helper()
		cur := d.Snapshot()
		if vs := CheckInvariants(prev, cur); len(vs) > 0 {
			msgs := make([]string, len(vs))
			for i, v := range vs {
				msgs[i] = v.String()
			}
			t.Fatalf("scheduler invariant violation at step (now=%s): %s",
				cur.Now.Format("2006-01-02T15:04:05"), strings.Join(msgs, "; "))
		}
		snap := cur
		prev = &snap
	}
}

// checkLedger checks that every start the scheduler buffered is either sent to
// StartWorkflowExecution or discarded on purpose, and never lost to a close.
func checkLedger(prev *Snapshot, cur Snapshot) []Violation {
	var vs violations

	if cur.Migrating && cur.UntransferableActions > 0 {
		vs.add("migration-untransferable", "%d execution/retry/cancel/terminate obligations lost native task ownership", cur.UntransferableActions)
	}
	if cur.Migrating && cur.Closed && len(cur.PendingStarts) > 0 {
		vs.add("migration-transfer-unverified", "closed source retains %d obligations without a verified destination", len(cur.PendingStarts))
	}

	// Terminal half: a schedule never closes while a start it buffered is
	// still waiting for InvokerExecuteTask (temporal#13016).
	if cur.Closed && !cur.Migrating && len(cur.PendingStarts) > 0 {
		vs.add("ledger-lost-start",
			"scheduler closed with %d buffered start(s) never sent to StartWorkflowExecution: %v",
			len(cur.PendingStarts), slices.Sorted(maps.Keys(cur.PendingStarts)))
	}
	if prev == nil {
		return vs
	}

	// Conservation half: every start that leaves the buffer without a RunId was
	// either sent to StartWorkflowExecution or discarded on purpose (overlap
	// skip, missed catchup, paused/limited drop). Starts that are buffered and
	// discarded within one transaction never appear pending, so the discard
	// counters can only exceed the disappearances, never trail. A start may also
	// be replaced by a CANCEL_OTHER or TERMINATE_OTHER start (see
	// Snapshot.ReplacingStarts).
	var silent []string
	for id := range prev.PendingStarts {
		_, still := cur.PendingStarts[id]
		_, sent := cur.StartRequested[id]
		if !still && !sent {
			silent = append(silent, id)
		}
	}
	discarded := (cur.OverlapSkipped - prev.OverlapSkipped) + (cur.DroppedStarts - prev.DroppedStarts) +
		int64(prev.ReplacingStarts+cur.ReplacingSeen-prev.ReplacingSeen)
	if int64(len(silent)) > discarded {
		slices.Sort(silent)
		vs.add("ledger-silent-drop",
			"%d buffered start(s) left the buffer without a start RPC, but only %d discard(s) were recorded: %v",
			len(silent), discarded, silent)
	}
	return vs
}

func checkStartAccounting(cur Snapshot) []Violation {
	var vs violations
	for id, accepted := range cur.AcceptedStarts {
		recorded, ok := cur.RecordedStarts[id]
		if ok && recorded != accepted {
			vs.add("start-identity", "request %s recorded %+v, accepted %+v", id, recorded, accepted)
		}
		if _, pending := cur.PendingStarts[id]; !ok && !pending {
			vs.add("accepted-start-lost", "accepted request %s has neither a pending obligation nor a recorded result", id)
		}
	}
	if want := cur.InitialActionCount + int64(len(cur.RecordedStarts)); cur.ActionCount != want {
		vs.add("start-accounting", "ActionCount=%d, observed recorded starts plus initial count=%d", cur.ActionCount, want)
	}
	return vs
}
