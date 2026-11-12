package chasmtest

import (
	"cmp"
	"context"
	"slices"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

// TaskRunner steps one execution on an [Engine] through the tasks it schedules
// for itself, moving a controllable clock forward. It stands in for the queue
// processors that deliver pure and side-effect tasks in production, so a test
// can drive a component through its whole lifecycle deterministically and check
// invariants after every settled transition.
//
// A step fires every pure task due at the current reference time (through
// [Engine.FirePureTasks]) and dispatches every side-effect task whose
// visibility time has arrived, then advances the clock to the next scheduled
// task. A component that keeps re-arming a timer never quiesces, so callers
// bound [TaskRunner.RunToQuiescence] with a step count.
//
// Component-specific behaviour (completing external work before the clock
// passes some deadline, counting RPCs, tolerating particular task errors,
// intercepting the engine a task handler sees) is plugged in through
// [TaskRunnerHooks] and [WithAsyncEngine].
//
// A TaskRunner is not safe for concurrent use. The exception is the
// Begin/Finish pair, which lets the test goroutine interleave with one task
// running on its own goroutine.
type TaskRunner struct {
	t      TB
	engine *Engine
	ref    chasm.ComponentRef
	clock  *clock.EventTimeSource

	maxSideEffectLag time.Duration
	hooks            TaskRunnerHooks
	wrapAsyncEngine  func(chasm.Engine) chasm.Engine

	// dispatched holds the physical side-effect tasks already handed out,
	// keyed by pointer identity: the engine's backend keeps every physical task
	// it ever generated, and executing a task does not remove it.
	dispatched map[*tasks.ChasmTask]struct{}

	// inFlight receives the result of the task started by BeginSideEffect.
	inFlight chan error
}

// TaskRunnerHooks are optional callbacks that let a component's test harness
// observe and extend the stepping loop. Any field may be nil. They run on the
// stepping goroutine, in the order the fields are listed for each kind of step.
//
// A side-effect dispatch runs BeforeSideEffect, executes the task, applies
// TolerateSideEffectError, runs AfterSideEffect, commits the transaction (which
// runs the engine's invariant checks), then runs AfterSideEffectCommit and
// AfterStep. A pure-task step fires the due tasks and commits, then runs
// AfterStep. BeforeAdvance runs ahead of every clock move.
type TaskRunnerHooks struct {
	// BeforeAdvance runs before the clock moves to the given time and may
	// itself move the clock to an earlier instant, for example to complete
	// external work that would otherwise run past a deadline. It runs even
	// when the clock does not end up moving.
	BeforeAdvance func(to time.Time)
	// BeforeSideEffect runs after a task has been selected and validated, just
	// before it executes.
	BeforeSideEffect func()
	// AfterSideEffect runs once the task returned, successfully or with a
	// tolerated error, before its transaction is committed.
	AfterSideEffect func()
	// AfterSideEffectCommit runs after a side-effect task's transaction is
	// committed, before AfterStep.
	AfterSideEffectCommit func()
	// AfterStep runs after every committed pure or side-effect step.
	AfterStep func()
	// TolerateSideEffectError reports whether an error returned by a
	// side-effect task is expected and should not fail the test. By default
	// every error fails the test.
	TolerateSideEffectError func(err error) bool
}

// TaskRunnerOption configures a [TaskRunner].
type TaskRunnerOption func(*TaskRunner)

// WithMaxSideEffectLag bounds how far [TaskRunner.StepPure] may move the clock
// past the oldest undispatched side-effect task, modelling a side-effect queue
// that lags behind the timers. Zero, the default, means unbounded.
func WithMaxSideEffectLag(lag time.Duration) TaskRunnerOption {
	return func(r *TaskRunner) { r.maxSideEffectLag = lag }
}

// WithTaskRunnerHooks installs hooks. See [TaskRunnerHooks].
func WithTaskRunnerHooks(hooks TaskRunnerHooks) TaskRunnerOption {
	return func(r *TaskRunner) { r.hooks = hooks }
}

// WithAsyncEngine wraps the engine that a task started with
// [TaskRunner.BeginSideEffect] sees, so a test can intercept the calls the
// handler makes back into the engine. Tasks dispatched through the Step methods
// always see the engine itself.
func WithAsyncEngine(wrap func(chasm.Engine) chasm.Engine) TaskRunnerOption {
	return func(r *TaskRunner) { r.wrapAsyncEngine = wrap }
}

// NewTaskRunner returns a runner for the execution identified by ref on e. The
// execution need not exist yet. ts must be the time source the engine was built
// with ([WithTimeSource]), since the runner moves it.
func NewTaskRunner(
	t TB,
	e *Engine,
	ref chasm.ComponentRef,
	ts *clock.EventTimeSource,
	opts ...TaskRunnerOption,
) *TaskRunner {
	r := &TaskRunner{
		t:          t,
		engine:     e,
		ref:        ref,
		clock:      ts,
		dispatched: make(map[*tasks.ChasmTask]struct{}),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Step advances the clock to the next scheduled task and fires all tasks due at
// that time: pure tasks first, then every due side-effect task. It returns false
// if nothing was pending, i.e. the execution is quiescent.
func (r *TaskRunner) Step() bool {
	r.t.Helper()

	fireTime, ok := r.nextFireTime()
	if !ok {
		return false
	}
	r.advanceTo(fireTime)
	r.firePureTasks()
	for r.StepSideEffect() {
	}
	return true
}

// StepPure advances the clock to the next pending pure task and fires every
// pure task due then, leaving side-effect tasks undispatched. This models the
// side-effect queue lagging behind the timers. It returns false if no pure task
// is pending, or if firing it would move the clock more than the configured
// [WithMaxSideEffectLag] past the oldest undispatched side-effect task.
func (r *TaskRunner) StepPure() bool {
	r.t.Helper()

	fireTime, ok := r.nextPureFireTime()
	if !ok {
		return false
	}
	if oldest, ok := r.oldestSideEffectTime(); ok && r.maxSideEffectLag > 0 &&
		fireTime.After(oldest.Add(r.maxSideEffectLag)) {
		return false
	}
	r.advanceTo(fireTime)
	r.firePureTasks()
	return true
}

// StepSideEffect dispatches the oldest side-effect task that is due at the
// current time, without moving the clock. Tasks that are no longer valid are
// skipped. It returns false if none is due.
func (r *TaskRunner) StepSideEffect() bool {
	r.t.Helper()

	next := r.nextSideEffect()
	if next == nil {
		return false
	}
	r.beforeSideEffect()
	err := r.executeSideEffect(context.Background(), r.engine, next)
	r.finishSideEffect(err)
	return true
}

// RunToQuiescence steps until no task is pending or maxSteps is reached. It
// returns the number of steps taken and whether the execution quiesced.
func (r *TaskRunner) RunToQuiescence(maxSteps int) (steps int, quiescent bool) {
	r.t.Helper()
	for steps = 0; steps < maxSteps; steps++ {
		if !r.Step() {
			return steps, true
		}
	}
	return steps, false
}

// HasPendingTask reports whether any task, pure or side-effect, is scheduled
// and still valid.
func (r *TaskRunner) HasPendingTask() bool {
	r.t.Helper()

	if _, ok := r.nextPureFireTime(); ok {
		return true
	}
	for _, task := range r.PendingSideEffectTasks() {
		if inTree, valid := r.ValidateSideEffectTask(task); inTree && valid {
			return true
		}
	}
	return false
}

// PendingPureTasks returns the pure tasks currently scheduled anywhere in the
// execution's tree, earliest first. The result is a copy.
func (r *TaskRunner) PendingPureTasks() []*persistencespb.ChasmComponentAttributes_Task {
	r.t.Helper()

	var out []*persistencespb.ChasmComponentAttributes_Task
	for _, node := range r.node().Snapshot(nil).Nodes {
		for _, task := range node.GetMetadata().GetComponentAttributes().GetPureTasks() {
			out = append(out, proto.Clone(task).(*persistencespb.ChasmComponentAttributes_Task))
		}
	}
	slices.SortStableFunc(out, func(a, b *persistencespb.ChasmComponentAttributes_Task) int {
		return a.GetScheduledTime().AsTime().Compare(b.GetScheduledTime().AsTime())
	})
	return out
}

// BeginSideEffect selects the next due side-effect task and runs it on its own
// goroutine, through the engine wrapped by [WithAsyncEngine], so the test can
// interleave with its calls back into the engine. It returns false if no task
// is due. The result arrives on [TaskRunner.SideEffectDone]; pass it to
// [TaskRunner.FinishSideEffect]. Only one task may be in flight, and the test
// may touch the engine meanwhile only while the handler is blocked in its
// wrapper.
func (r *TaskRunner) BeginSideEffect() bool {
	r.t.Helper()

	if r.inFlight != nil {
		r.t.Fatalf("a side-effect task is already in flight")
	}
	next := r.nextSideEffect()
	if next == nil {
		return false
	}
	r.beforeSideEffect()
	done := make(chan error, 1)
	r.inFlight = done
	ctx, cancel := context.WithCancel(r.t.Context())
	r.t.Cleanup(func() {
		cancel()
		if r.inFlight == done {
			<-done
			r.inFlight = nil
		}
	})
	var engine chasm.Engine = r.engine
	if r.wrapAsyncEngine != nil {
		engine = r.wrapAsyncEngine(engine)
	}
	go func() { done <- r.executeSideEffect(ctx, engine, next) }()
	return true
}

// SideEffectDone returns the channel on which the task started by
// [TaskRunner.BeginSideEffect] reports its result, or nil if none is in flight.
func (r *TaskRunner) SideEffectDone() <-chan error {
	return r.inFlight
}

// FinishSideEffect completes the in-flight task after its result err was
// received from [TaskRunner.SideEffectDone]: it checks the error, commits the
// transaction and runs the after-step hooks.
func (r *TaskRunner) FinishSideEffect(err error) {
	r.t.Helper()
	r.inFlight = nil
	r.finishSideEffect(err)
}

// AbandonSideEffect forgets the in-flight task after its result was received
// from [TaskRunner.SideEffectDone] but will not be finished, typically because
// the test is about to fail.
func (r *TaskRunner) AbandonSideEffect() {
	r.inFlight = nil
}

func (r *TaskRunner) node() *chasm.Node {
	r.t.Helper()
	exec, err := r.engine.executionForRef(r.ref)
	if err != nil {
		r.t.Fatalf("execution lookup failed: %v", err)
	}
	return exec.node
}

// nextPureFireTime returns the earliest scheduled pure task time in the tree.
//
// The tree's own pure-task metadata is the source rather than the engine
// backend's record of the last physical-task deletion: when a transaction
// closes, the node derives that recorded time from exactly this minimum over the
// first pure task of each component, so the two agree whenever the tree is
// clean, which it is between steps.
func (r *TaskRunner) nextPureFireTime() (time.Time, bool) {
	r.t.Helper()
	var earliest time.Time
	have := false
	for _, node := range r.node().Snapshot(nil).Nodes {
		pure := node.GetMetadata().GetComponentAttributes().GetPureTasks()
		if len(pure) == 0 {
			continue
		}
		if at := pure[0].GetScheduledTime().AsTime(); !have || at.Before(earliest) {
			earliest, have = at, true
		}
	}
	return earliest, have
}

// oldestSideEffectTime returns the earliest visibility time among undispatched
// side-effect tasks, if any.
func (r *TaskRunner) oldestSideEffectTime() (time.Time, bool) {
	var oldest time.Time
	have := false
	for _, task := range r.PendingSideEffectTasks() {
		if vt := task.GetVisibilityTime(); !have || vt.Before(oldest) {
			oldest, have = vt, true
		}
	}
	return oldest, have
}

// nextFireTime returns the earliest time at which any pending task is due.
func (r *TaskRunner) nextFireTime() (time.Time, bool) {
	pure, havePure := r.nextPureFireTime()
	side, haveSide := r.oldestSideEffectTime()
	switch {
	case havePure && haveSide:
		if side.Before(pure) {
			return side, true
		}
		return pure, true
	case havePure:
		return pure, true
	default:
		return side, haveSide
	}
}

// PendingSideEffectTasks returns the physical side-effect tasks this runner has
// not yet handed out, in the order the production queue would deliver them. A
// task may be pending yet no longer valid; see [TaskRunner.ValidateSideEffectTask].
func (r *TaskRunner) PendingSideEffectTasks() []*tasks.ChasmTask {
	exec, err := r.engine.executionForRef(r.ref)
	if err != nil {
		r.t.Fatalf("execution lookup failed: %v", err)
	}
	var out []*tasks.ChasmTask
	for category, categoryTasks := range exec.backend.TasksByCategory {
		// Visibility tasks are framework-managed (indexing search attributes) and
		// processed by a separate queue, not the CHASM side-effect executor; their
		// handler panics if invoked directly. Skip them.
		if category.ID() == tasks.CategoryVisibility.ID() {
			continue
		}
		for _, t := range categoryTasks {
			ct, ok := t.(*tasks.ChasmTask)
			if !ok {
				continue
			}
			if _, done := r.dispatched[ct]; done {
				continue
			}
			out = append(out, ct)
		}
	}
	slices.SortStableFunc(out, func(a, b *tasks.ChasmTask) int {
		if c := a.GetVisibilityTime().Compare(b.GetVisibilityTime()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Info.GetTaskVersionedTransition().GetTransitionCount(), b.Info.GetTaskVersionedTransition().GetTransitionCount()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Info.GetTaskVersionedTransitionOffset(), b.Info.GetTaskVersionedTransitionOffset()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Info.GetTypeId(), b.Info.GetTypeId()); c != 0 {
			return c
		}
		return slices.Compare(a.Info.GetPath(), b.Info.GetPath())
	})
	return out
}

// ValidateSideEffectTask reports whether the physical task still corresponds to
// a logical task in the tree (inTree), and whether that logical task's handler
// still considers it valid. It does not mark the task as dispatched.
func (r *TaskRunner) ValidateSideEffectTask(task *tasks.ChasmTask) (inTree, valid bool) {
	r.t.Helper()
	inTree, valid, err := r.node().ValidateSideEffectTask(context.Background(), task)
	if err != nil {
		r.t.Fatalf("ValidateSideEffectTask: %v", err)
	}
	return inTree, valid
}

// nextSideEffect returns the oldest due, valid side-effect task and marks it
// and every invalid task before it as dispatched.
func (r *TaskRunner) nextSideEffect() *tasks.ChasmTask {
	r.t.Helper()
	for _, task := range r.PendingSideEffectTasks() {
		if task.GetVisibilityTime().After(r.clock.Now()) {
			continue
		}
		r.dispatched[task] = struct{}{}
		if inTree, valid := r.ValidateSideEffectTask(task); inTree && valid {
			return task
		}
	}
	return nil
}

func (r *TaskRunner) executeSideEffect(ctx context.Context, engine chasm.Engine, task *tasks.ChasmTask) error {
	return r.node().ExecuteSideEffectTask(
		chasm.NewEngineContext(ctx, engine), r.ref.ExecutionKey, task,
		func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil },
	)
}

func (r *TaskRunner) beforeSideEffect() {
	if r.hooks.BeforeSideEffect != nil {
		r.hooks.BeforeSideEffect()
	}
}

func (r *TaskRunner) finishSideEffect(err error) {
	r.t.Helper()
	if err != nil && (r.hooks.TolerateSideEffectError == nil || !r.hooks.TolerateSideEffectError(err)) {
		r.t.Fatalf("ExecuteSideEffectTask failed: %v", err)
	}
	if r.hooks.AfterSideEffect != nil {
		r.hooks.AfterSideEffect()
	}
	r.commit()
	if r.hooks.AfterSideEffectCommit != nil {
		r.hooks.AfterSideEffectCommit()
	}
	r.afterStep()
}

// firePureTasks fires every pure task due at the current time and commits.
func (r *TaskRunner) firePureTasks() {
	r.t.Helper()
	if _, err := r.engine.FirePureTasks(r.ref, r.clock.Now()); err != nil {
		r.t.Fatalf("FirePureTasks failed: %v", err)
	}
	r.afterStep()
}

func (r *TaskRunner) commit() {
	r.t.Helper()
	exec, err := r.engine.executionForRef(r.ref)
	if err == nil {
		err = r.engine.closeTransaction(context.Background(), exec)
	}
	if err != nil {
		r.t.Fatalf("closing transaction: %v", err)
	}
}

func (r *TaskRunner) afterStep() {
	if r.hooks.AfterStep != nil {
		r.hooks.AfterStep()
	}
}

// advanceTo moves the clock to t, never backward, after giving BeforeAdvance a
// chance to act on the way.
func (r *TaskRunner) advanceTo(t time.Time) {
	if r.hooks.BeforeAdvance != nil {
		r.hooks.BeforeAdvance(t)
	}
	if t.After(r.clock.Now()) {
		r.clock.Update(t)
	}
}
