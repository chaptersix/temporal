package schedulertest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
)

// This file implements the "stuckness" and idle-deadline invariants for
// Schedules/V2. [SchedulerEngine] installs them on every transition it commits,
// and the unit tests' testEnv asserts them on every CloseTransaction, so the
// whole suite and the lifecycle property check the same two predicates.
//
// Production has repeatedly produced schedules that are open, unpaused, and
// carry no pending work: they never re-arm and never close. Every confirmed
// instance violated exactly one predicate:
//
//	  !sched.Closed  =>  the tree carries at least one live logical task.
//
// This is a liveness property, not a transition-validity property. The
// scheduler has no state enum to validate transitions against:
// Scheduler.LifecycleState is derived and two-valued (Completed iff Closed),
// and every sub-component hardcodes LifecycleStateRunning. In each known bug
// every transition taken was legal; the failure was that no transition
// happened at all.
//
// The stuck predicate reads only plain Go field access on the embedded
// *schedulerpb.SchedulerState proto and the serialized snapshot returned by
// Node.Snapshot. It never constructs a chasm.Context, because materializing a
// chasm Field (e.g. sched.Invoker.Get(ctx)) escalates the root node's
// valueState and marks the tree dirty even for a read. Only the idle-task
// validity check does, see idleTaskValid.

// schedState is a test-only vocabulary for the scheduler's lifecycle. It is
// derived, never persisted: there is deliberately no state enum on the
// component, and adding one is a proto change with V1->V2 migration and
// rollback implications that this check does not require.
type schedState string

const (
	// stateSentinel is a schedule-ID reservation. Inert by design.
	stateSentinel schedState = "sentinel"
	// stateMigrating is mid-migration to a workflow-backed (V1) scheduler,
	// driven by SchedulerMigrateToWorkflowTask.
	stateMigrating schedState = "migrating"
	// stateCompleted is terminal: Closed is set.
	stateCompleted schedState = "completed"
	// stateIdle is non-terminal and quiescent, awaiting its idle timer.
	stateIdle schedState = "idle"
	// stateRunning is non-terminal with pending work armed.
	stateRunning schedState = "running"
	// stateStuck is the bug: non-terminal, but nothing will ever wake it.
	stateStuck schedState = "stuck"
)

// StateInvariant asserts the stuck and idle-deadline invariants against a clean
// tree. Build one with [NewStateInvariant]; [NewEngine] installs one on the
// engine it builds and exposes it as [SchedulerEngine.State].
type StateInvariant struct {
	registry    *chasm.Registry
	idleHandler *scheduler.SchedulerIdleTaskHandler

	mu               sync.Mutex
	allowStuckReason string
}

// NewStateInvariant builds the check for trees registered in registry.
// config and logger back the idle task handler used to validate armed idle
// tasks.
func NewStateInvariant(registry *chasm.Registry, config *scheduler.Config, logger log.Logger) *StateInvariant {
	return &StateInvariant{
		registry: registry,
		idleHandler: scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
		}),
	}
}

// AllowStuck opts out of both checks, for tests that deliberately drive the
// scheduler into a state with no pending work.
//
// reason is mandatory: an unexplained opt-out is indistinguishable from a
// silently tolerated bug, and the set of tests that need one is itself a
// finding worth reviewing.
func (c *StateInvariant) AllowStuck(t chasmtest.TB, reason string) {
	t.Helper()
	if reason == "" {
		t.Fatal("AllowStuck requires a reason explaining why this test tolerates a stuck scheduler")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.allowStuckReason = reason
}

// Check fails t if the scheduler in a clean tree is stuck, or advertises an idle
// deadline that no valid idle task backs. node must be the root and the tree
// must be clean; both hold immediately after a successful CloseTransaction.
func (c *StateInvariant) Check(t chasmtest.TB, node *chasm.Node, sched *scheduler.Scheduler) {
	t.Helper()
	if vs := c.Violations(node, sched); len(vs) > 0 {
		t.Fatalf("%s", strings.Join(vs, "\n"))
	}
}

// Violations reports every broken predicate without failing a test, so a test
// that corrupts state on purpose can assert the check catches it. It reports
// nothing once [StateInvariant.AllowStuck] was called.
func (c *StateInvariant) Violations(node *chasm.Node, sched *scheduler.Scheduler) []string {
	c.mu.Lock()
	allowed := c.allowStuckReason != ""
	c.mu.Unlock()
	if allowed {
		return nil
	}

	var out []string
	total, byPath := liveTaskCount(node)
	if derivedState(sched, total) == stateStuck {
		out = append(out, fmt.Sprintf("scheduler is stuck: %s", describeSched(sched, total, byPath)))
	}
	if v := c.idleViolation(node, sched, total, byPath); v != "" {
		out = append(out, v)
	}
	return out
}

// idleViolation asserts that a declared idle deadline is backed by an armed
// idle task that its handler still considers valid.
//
// IdleCloseTime is surfaced as the ScheduleIdleCloseTime search attribute and
// is what the production stuck-schedule scanner reads. When
// SchedulerIdleTaskHandler.Validate drops a task as expiration_shift and
// nothing arms a replacement, IdleCloseTime is left behind: the schedule never
// closes, and the very signal built to detect that reports a close time that
// will never arrive. Counting armed tasks is not enough, since an idle task the
// handler drops (deadline moved, schedule held open) leaves the same hole, so
// the armed tasks are run through Validate.
//
// Only asserted in this direction. Sentinels legitimately arm an idle task
// without setting IdleCloseTime (see NewSentinel), so an armed task with no
// declared deadline is not a defect.
func (c *StateInvariant) idleViolation(node *chasm.Node, sched *scheduler.Scheduler, total int, byPath map[string]int) string {
	if sched.GetSentinel() || sched.GetClosed() || sched.GetIdleCloseTime() == nil {
		return ""
	}
	if c.idleTaskValid(node, sched) {
		return ""
	}
	return fmt.Sprintf("IdleCloseTime is set to %v but no valid SchedulerIdleTask is armed (%d armed), so the "+
		"schedule will never close and ScheduleIdleCloseTime reports a deadline that will never arrive: %s",
		sched.GetIdleCloseTime().AsTime(), IdleTaskCount(c.registry, node), describeSched(sched, total, byPath))
}

// idleTaskValid reports whether any armed idle task passes the handler's
// Validate. It validates through a read-only context over the clean tree.
func (c *StateInvariant) idleTaskValid(node *chasm.Node, sched *scheduler.Scheduler) bool {
	wantTypeID, ok := c.registry.TaskIDFor(&schedulerpb.SchedulerIdleTask{})
	if !ok {
		return false
	}
	ctx := chasm.NewContext(context.Background(), node)
	for _, n := range node.Snapshot(nil).Nodes {
		for _, task := range n.GetMetadata().GetComponentAttributes().GetPureTasks() {
			if task.GetTypeId() != wantTypeID {
				continue
			}
			value := &schedulerpb.SchedulerIdleTask{}
			if err := proto.Unmarshal(task.GetData().GetData(), value); err != nil {
				return false
			}
			valid, err := c.idleHandler.Validate(ctx, sched, chasm.TaskInvocation{
				TaskAttributes: chasm.TaskAttributes{ScheduledTime: task.GetScheduledTime().AsTime()},
			}, value)
			if err == nil && valid {
				return true
			}
		}
	}
	return false
}

// liveTaskCount counts logical tasks across every node in the tree.
//
// This deliberately counts *logical* tasks, from ChasmComponentAttributes.
// PureTasks and SideEffectTasks, rather than physical tasks. A physical task
// can exist on the backend while the logical task it points at has been reaped
// or invalidated, so counting physical tasks reports a schedule as armed when
// it is in fact stuck. Side-effect tasks count because some valid states, such
// as migration callback attachment, rely on one before arming pure tasks.
// (chasmtest.Engine.Tasks and testEnv.NodeBackend.TasksByCategory both expose
// physical tasks, and are the wrong source for this check.)
//
// Note that the framework-managed visibility task counts here, as it always
// has. A test engine never dispatches it, so it stays in the tree after any
// transition that touches a search attribute and masks a scheduler with nothing
// else left to run. Excluding it exposes a hand-built taskless state in
// TestGeneratorTask_EngineHighWaterMarkInvalidation, so the lifecycle driver
// keeps its own no-stuck rule, which ignores it (see CheckInvariants).
//
// node must be the root and the tree must be clean; Node.Snapshot panics
// otherwise. Both hold immediately after a successful CloseTransaction.
func liveTaskCount(node *chasm.Node) (int, map[string]int) {
	byPath := make(map[string]int)
	total := 0
	for path, n := range node.Snapshot(nil).Nodes {
		attrs := n.GetMetadata().GetComponentAttributes()
		taskCount := len(attrs.GetPureTasks()) + len(attrs.GetSideEffectTasks())
		if taskCount == 0 {
			continue
		}
		byPath[path] = taskCount
		total += taskCount
	}
	return total, byPath
}

// derivedState computes the scheduler's lifecycle state from the tuple that
// actually represents it: (Sentinel, WorkflowMigration, Closed, IdleCloseTime,
// live logical task count).
func derivedState(sched *scheduler.Scheduler, liveTasks int) schedState {
	switch {
	case sched.GetSentinel():
		return stateSentinel
	case sched.GetWorkflowMigration() != nil:
		return stateMigrating
	case sched.GetClosed():
		return stateCompleted
	case liveTasks == 0:
		return stateStuck
	case sched.GetIdleCloseTime() != nil:
		return stateIdle
	default:
		return stateRunning
	}
}

// IdleTaskCount counts armed SchedulerIdleTasks across the tree, resolving the
// task's registered type ID rather than matching on node path (the root
// Scheduler component also carries callback and migration tasks). It returns -1
// if the idle task is not registered.
func IdleTaskCount(registry *chasm.Registry, node *chasm.Node) int {
	wantTypeID, ok := registry.TaskIDFor(&schedulerpb.SchedulerIdleTask{})
	if !ok {
		return -1
	}
	count := 0
	for _, n := range node.Snapshot(nil).Nodes {
		for _, task := range n.GetMetadata().GetComponentAttributes().GetPureTasks() {
			if task.GetTypeId() == wantTypeID {
				count++
			}
		}
	}
	return count
}

// describeSched renders the full state tuple for a failure message, so a
// triage does not require re-running under a debugger.
func describeSched(sched *scheduler.Scheduler, total int, byPath map[string]int) string {
	paths := make([]string, 0, len(byPath))
	for p := range byPath {
		paths = append(paths, fmt.Sprintf("%s=%d", p, byPath[p]))
	}
	slices.Sort(paths)

	var b strings.Builder
	fmt.Fprintf(&b, "\n  scheduleID:     %s", sched.GetScheduleId())
	fmt.Fprintf(&b, "\n  closed:         %v", sched.GetClosed())
	fmt.Fprintf(&b, "\n  sentinel:       %v", sched.GetSentinel())
	fmt.Fprintf(&b, "\n  paused:         %v", sched.GetSchedule().GetState().GetPaused())
	fmt.Fprintf(&b, "\n  limitedActions: %v", sched.GetSchedule().GetState().GetLimitedActions())
	fmt.Fprintf(&b, "\n  remaining:      %d", sched.GetSchedule().GetState().GetRemainingActions())
	fmt.Fprintf(&b, "\n  conflictToken:  %d", sched.GetConflictToken())
	fmt.Fprintf(&b, "\n  idleCloseTime:  %v", sched.GetIdleCloseTime().AsTime())
	fmt.Fprintf(&b, "\n  migration:      %v", sched.GetWorkflowMigration() != nil)
	fmt.Fprintf(&b, "\n  liveTasks:      %d [%s]", total, strings.Join(paths, " "))
	return b.String()
}
