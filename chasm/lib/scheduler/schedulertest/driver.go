package schedulertest

import (
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Driver steps a real CHASM scheduler through every task it schedules for
// itself, advancing a controllable clock. It is a [SchedulerEngine] plus what is
// specific to driving the scheduler's lifecycle: fake frontend and history
// services with an external ledger and fault injection, the hooks that observe
// each step, and the operations a customer or the system performs on a schedule.
// After each settled transition it invokes AfterStep (if set), which is where
// invariant assertions are plugged in.
//
// A "step", run by a [chasmtest.TaskRunner], fires every pure task due at the
// current reference time and every side-effect task whose visibility time has
// arrived, then advances the clock to the next scheduled task. A healthy interval
// schedule never truly quiesces (it always re-arms a generator task), so callers
// bound the run with maxSteps; quiescence is reached only for paused, exhausted
// or closed schedules.
type Driver struct {
	*SchedulerEngine
	*fakeServices

	recordCalls        chan *RecordCall
	recorded           map[string]ExternalStart
	replacingSeen      map[string]struct{}
	initialActionCount int64

	// metricsCapture records the scheduler's metrics; with the start ledger it
	// backs the accounting invariants.
	metricsCapture *metricstest.Capture

	// runner steps the execution through its pure and side-effect tasks.
	runner *chasmtest.TaskRunner

	// actionsBefore is the action RPC count when the current side-effect task
	// began. maxActionsObserved is the high-water mark of action RPCs made by any
	// single side-effect task dispatch (one InvokerExecuteTask), which backs the
	// action-budget invariant.
	actionsBefore      int
	maxActionsObserved int

	// maxRunDuration auto-completes a started workflow before the clock moves
	// past its StartTime plus this. Zero means workflows only complete through
	// CompleteWorkflow.
	maxRunDuration time.Duration

	// AfterStep, if set, is called after every step's CloseTransaction with the
	// driver, so invariant checks can observe each settled state.
	AfterStep func(d *Driver)
}

type driverConfig struct {
	schedule         *schedulepb.Schedule
	startTime        time.Time
	afterStep        func(d *Driver)
	config           *scheduler.Config
	maxLag           time.Duration
	maxRun           time.Duration
	controlledStarts bool
	noStateCheck     bool
}

// DriverOption configures a Driver at construction.
type DriverOption func(*driverConfig)

// WithSchedule overrides the schedule definition (default: DefaultSchedule).
func WithSchedule(s *schedulepb.Schedule) DriverOption {
	return func(c *driverConfig) { c.schedule = s }
}

// WithStartTime sets the wall-clock instant the engine's clock starts at.
func WithStartTime(t time.Time) DriverOption {
	return func(c *driverConfig) { c.startTime = t }
}

// WithAfterStep installs a hook called after every step.
func WithAfterStep(fn func(d *Driver)) DriverOption {
	return func(c *driverConfig) { c.afterStep = fn }
}

// WithConfig overrides the scheduler config (default: DefaultConfig). Use it to
// set small Tweakables — e.g. MaxBufferSize or MaxActionsPerExecution — so the
// buffer-bound and action-budget invariants are meaningfully exercised.
func WithConfig(cfg *scheduler.Config) DriverOption {
	return func(c *driverConfig) { c.config = cfg }
}

// WithoutStateInvariant leaves the shared stuck and idle-deadline invariants
// off the engine, which otherwise fails the test the moment a transition commits
// a violating state. Use it only in a test that corrupts the scheduler on
// purpose, and assert Driver.StateViolations instead.
func WithoutStateInvariant() DriverOption {
	return func(c *driverConfig) { c.noStateCheck = true }
}

// WithMaxSideEffectLag bounds how long StepPure may leave a side-effect task
// (e.g. InvokerExecuteTask) undispatched while moving the clock forward.
func WithMaxSideEffectLag(lag time.Duration) DriverOption {
	return func(c *driverConfig) { c.maxLag = lag }
}

// WithMaxRunDuration bounds how long a started workflow runs: the driver
// completes it before moving the clock past StartTime plus d.
func WithMaxRunDuration(d time.Duration) DriverOption {
	return func(c *driverConfig) { c.maxRun = d }
}

// NewDriver constructs a Driver and starts a scheduler execution via the
// production CreateScheduler path. startTime defaults to a fixed instant for
// determinism.
func NewDriver(t chasmtest.TB, opts ...DriverOption) *Driver {
	t.Helper()
	d, cfg := newDriverScaffold(t, opts...)
	req := &schedulerpb.CreateScheduleRequest{
		NamespaceId: NamespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  Namespace,
			ScheduleId: ScheduleID,
			Schedule:   cfg.schedule,
			RequestId:  CreateRequestID,
		},
	}
	d.start(func(mctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		return scheduler.CreateScheduler(mctx, req)
	})
	return d
}

// NewDriverFromMigration constructs a Driver and starts a scheduler execution
// from a migrated V1 state via the production CreateSchedulerFromMigration path.
// This is the V1->V2 import side of a migration round trip.
func NewDriverFromMigration(t chasmtest.TB, req *schedulerpb.CreateFromMigrationStateRequest, opts ...DriverOption) *Driver {
	t.Helper()
	d, _ := newDriverScaffold(t, opts...)
	d.start(func(mctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		return scheduler.CreateSchedulerFromMigration(mctx, req)
	})
	return d
}

// newDriverScaffold builds the engine, fake services and clock, but does not
// start an execution.
func newDriverScaffold(t chasmtest.TB, opts ...DriverOption) (*Driver, *driverConfig) {
	t.Helper()

	cfg := &driverConfig{
		schedule:  DefaultSchedule(),
		startTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		config:    DefaultConfig(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	ts := clock.NewEventTimeSource()
	ts.Update(cfg.startTime)
	services := newFakeServices(t, gomock.NewController(t), ts, cfg.controlledStarts)
	metricsHandler := metricstest.NewCaptureHandler()

	engineOpts := []Option{
		WithLogger(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)),
		WithSchedulerConfig(cfg.config),
		WithTimeSource(ts),
		WithInvokerTimeSource(ts),
		WithInvokerMetrics(metricsHandler),
		WithClients(services.Frontend, services.History),
	}
	if cfg.noStateCheck {
		engineOpts = append(engineOpts, WithoutStateCheck())
	}

	d := &Driver{
		SchedulerEngine: NewEngine(t, engineOpts...),
		fakeServices:    services,
		recordCalls:     make(chan *RecordCall),
		recorded:        make(map[string]ExternalStart),
		replacingSeen:   make(map[string]struct{}),
		metricsCapture:  metricsHandler.StartCapture(),
		maxRunDuration:  cfg.maxRun,
		AfterStep:       cfg.afterStep,
	}
	d.runner = d.newTaskRunner(cfg.maxLag)
	return d, cfg
}

// start starts the scheduler execution using the given start function.
func (d *Driver) start(startFn func(chasm.MutableContext) (*scheduler.Scheduler, error)) {
	d.T.Helper()
	if err := d.Start(startFn); err != nil {
		d.T.Fatalf("failed to start scheduler execution: %v", err)
	}
	d.ReadScheduler(func(s *scheduler.Scheduler, _ chasm.Context) { d.initialActionCount = s.Info.GetActionCount() })
}

// Now returns the engine's current clock time.
func (d *Driver) Now() time.Time {
	return d.TimeSource.Now()
}

// ReadScheduler reads the scheduler root component and invokes fn with it.
func (d *Driver) ReadScheduler(fn func(s *scheduler.Scheduler, ctx chasm.Context)) {
	d.T.Helper()
	err := d.Read(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		fn(s, ctx)
		return nil
	})
	if err != nil {
		d.T.Fatalf("ReadScheduler failed: %v", err)
	}
}

// Step advances the clock to the next scheduled task and fires all tasks due at
// that time: pure tasks first, then every due side-effect task. It returns false
// if nothing was pending (the schedule is quiescent).
func (d *Driver) Step() bool {
	d.T.Helper()
	return d.runner.Step()
}

// StepPure advances the clock to the next pending pure task and fires every pure
// task due then, leaving side-effect tasks undispatched. This models
// InvokerExecuteTask lagging behind the scheduler's timers. It returns false if
// no pure task is pending, or if firing it would move the clock more than
// the configured max side-effect lag past the oldest undispatched side-effect task.
func (d *Driver) StepPure() bool {
	d.T.Helper()
	return d.runner.StepPure()
}

// StepSideEffect dispatches the oldest side-effect task that is due at the
// current time, without moving the clock. It returns false if none is due.
func (d *Driver) StepSideEffect() bool {
	d.T.Helper()
	return d.runner.StepSideEffect()
}

// RunToQuiescence steps until no task is pending or maxSteps is reached. It
// returns the number of steps taken and whether the schedule reached quiescence
// (no pending task). A normal running schedule never quiesces and will stop at
// maxSteps with quiescent == false.
func (d *Driver) RunToQuiescence(maxSteps int) (steps int, quiescent bool) {
	d.T.Helper()
	return d.runner.RunToQuiescence(maxSteps)
}

// HasPendingTask reports whether any task (pure or side-effect) is currently
// scheduled to fire.
func (d *Driver) HasPendingTask() bool {
	d.T.Helper()
	return d.runner.HasPendingTask()
}

// newTaskRunner wires the scheduler-specific behaviour into a generic task
// runner.
func (d *Driver) newTaskRunner(maxLag time.Duration) *chasmtest.TaskRunner {
	return chasmtest.NewTaskRunner(d.T, d.Engine, d.Ref, d.TimeSource,
		chasmtest.WithMaxSideEffectLag(maxLag),
		chasmtest.WithAsyncEngine(func(engine chasm.Engine) chasm.Engine {
			return &recordingEngine{Engine: engine, calls: d.recordCalls}
		}),
		chasmtest.WithTaskRunnerHooks(chasmtest.TaskRunnerHooks{
			BeforeAdvance:    d.completeWorkflowsBefore,
			BeforeSideEffect: func() { d.actionsBefore = d.actionCount() },
			// A side-effect task may find the schedule already closed and gone.
			TolerateSideEffectError: func(err error) bool {
				_, ok := errors.AsType[*serviceerror.NotFound](err)
				return ok
			},
			AfterSideEffect:       func() { d.maxActionsObserved = max(d.maxActionsObserved, d.actionCount()-d.actionsBefore) },
			AfterSideEffectCommit: d.observeRecordedStarts,
			AfterStep:             d.runAfterStep,
		}),
	)
}

func (d *Driver) runAfterStep() {
	if d.AfterStep != nil {
		d.AfterStep(d)
	}
}

// completeWorkflowsBefore completes any workflow that would exceed
// maxRunDuration before the clock moves to t, moving the clock to the deadline
// first.
func (d *Driver) completeWorkflowsBefore(t time.Time) {
	for d.maxRunDuration > 0 {
		startTime, ok := d.oldestRunningStartTime()
		if !ok {
			break
		}
		due := startTime.Add(d.maxRunDuration)
		if due.After(t) {
			break
		}
		if due.After(d.Now()) {
			d.TimeSource.Update(due)
		}
		d.CompleteWorkflow()
		// The completion may ready a deferred start; don't skip its execute task.
		for d.StepSideEffect() {
		}
	}
}

// oldestRunningStartTime returns the StartTime of the oldest started workflow
// that hasn't completed.
func (d *Driver) oldestRunningStartTime() (time.Time, bool) {
	var oldest time.Time
	have := false
	d.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		if s.Closed || s.IsSentinel() {
			return
		}
		if start := oldestRunning(s.Invoker.Get(ctx).GetBufferedStarts()); start != nil {
			oldest, have = start.GetStartTime().AsTime(), true
		}
	})
	return oldest, have
}

// oldestRunning returns the earliest-started buffered start that has a run but
// has not completed, or nil.
func oldestRunning(starts []*schedulespb.BufferedStart) *schedulespb.BufferedStart {
	var oldest *schedulespb.BufferedStart
	for _, start := range starts {
		if start.GetRunId() == "" || start.GetCompleted() != nil {
			continue
		}
		if oldest == nil || start.GetStartTime().AsTime().Before(oldest.GetStartTime().AsTime()) {
			oldest = start
		}
	}
	return oldest
}

// StateViolations evaluates the shared stuck and idle-deadline invariants
// against the last committed state without failing the test. See
// [WithoutStateInvariant].
func (d *Driver) StateViolations() []string {
	d.T.Helper()
	return d.SchedulerEngine.StateViolations()
}

// apply runs op against the scheduler root in a single transaction. On success
// the AfterStep hook runs, so invariants are checked at the new settled state.
// It returns the operation's error without failing the test, so callers driving
// random operations can tolerate rejected ones (e.g. ErrClosed once the
// schedule has closed).
func (d *Driver) apply(op func(*scheduler.Scheduler, chasm.MutableContext) error) error {
	d.T.Helper()
	err := d.SchedulerEngine.Update(op)
	if err == nil {
		d.runAfterStep()
	}
	return err
}

// CompleteWorkflow delivers a successful completion callback for the oldest
// running workflow, through Scheduler.HandleNexusCompletion. It returns false
// if no workflow is running.
func (d *Driver) CompleteWorkflow() bool {
	d.T.Helper()
	completed := false
	err := d.SchedulerEngine.Update(func(s *scheduler.Scheduler, mctx chasm.MutableContext) error {
		if s.Closed || s.IsSentinel() {
			return nil
		}
		oldest := oldestRunning(s.Invoker.Get(mctx).GetBufferedStarts())
		if oldest == nil {
			return nil
		}
		completed = true
		return s.HandleNexusCompletion(mctx, &persistencespb.ChasmNexusCompletion{
			RequestId: oldest.GetRequestId(),
			Outcome:   &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}},
			CloseTime: timestamppb.New(d.Now()),
		})
	})
	if err != nil {
		d.T.Fatalf("CompleteWorkflow failed: %v", err)
	}
	if completed {
		d.runAfterStep()
	}
	return completed
}

// DeliverCompletion delivers a completion callback for requestID. It also
// supports accepted starts whose RPC results have not been recorded locally, and
// duplicate or obsolete callback request identities.
func (d *Driver) DeliverCompletion(requestID string) error {
	return d.SchedulerEngine.Update(func(s *scheduler.Scheduler, mctx chasm.MutableContext) error {
		return s.HandleNexusCompletion(mctx, &persistencespb.ChasmNexusCompletion{
			RequestId: requestID, CloseTime: timestamppb.New(d.Now()),
			Outcome: &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}},
		})
	})
}

// Patch applies a SchedulePatch through the production Scheduler.Patch path in a
// single transaction. It returns the scheduler's error (e.g. ErrClosed when the
// schedule has already closed) without failing the test. On success the
// AfterStep hook runs.
func (d *Driver) Patch(patch *schedulepb.SchedulePatch) error {
	d.T.Helper()
	return d.apply(func(s *scheduler.Scheduler, mctx chasm.MutableContext) error {
		_, err := s.Patch(mctx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: NamespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  Namespace,
				ScheduleId: ScheduleID,
				Patch:      patch,
			},
		})
		return err
	})
}

// Update applies mutate to a copy of the current schedule definition and
// replaces the schedule with it through the production Scheduler.Update path,
// without a conflict token. It returns the scheduler's error (e.g. ErrClosed)
// without failing the test. On success the AfterStep hook runs. (The embedded
// SchedulerEngine.Update is the lower-level typed helper.)
func (d *Driver) Update(mutate func(*schedulepb.Schedule)) error {
	d.T.Helper()
	return d.apply(func(s *scheduler.Scheduler, mctx chasm.MutableContext) error {
		next := common.CloneProto(s.Schedule)
		mutate(next)
		_, err := s.Update(mctx, &schedulerpb.UpdateScheduleRequest{
			NamespaceId: NamespaceID,
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace:  Namespace,
				ScheduleId: ScheduleID,
				Schedule:   next,
			},
		})
		return err
	})
}

// Backfill requests a backfill of the given range via Patch.
func (d *Driver) Backfill(req *schedulepb.BackfillRequest) error {
	return d.Patch(&schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{req}})
}

// Pause pauses the schedule via Patch.
func (d *Driver) Pause() error {
	return d.Patch(&schedulepb.SchedulePatch{Pause: "paused by driver"})
}

// Unpause unpauses the schedule via Patch.
func (d *Driver) Unpause() error {
	return d.Patch(&schedulepb.SchedulePatch{Unpause: "unpaused by driver"})
}

// TriggerImmediately requests an immediate action via Patch.
func (d *Driver) TriggerImmediately() error {
	return d.Patch(&schedulepb.SchedulePatch{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
	})
}

// MigrateToWorkflow initiates a V2->V1 migration: it pauses the schedule and
// schedules the SchedulerMigrateToWorkflowTask side-effect task. Step the driver
// afterward to fire the task (which calls the history client's
// StartWorkflowExecution with the exported StartScheduleArgs). It does not run
// the AfterStep hook.
func (d *Driver) MigrateToWorkflow() error {
	d.T.Helper()
	return d.SchedulerEngine.Update(func(s *scheduler.Scheduler, mctx chasm.MutableContext) error {
		_, err := s.MigrateToWorkflow(mctx, &schedulerpb.MigrateToWorkflowRequest{
			NamespaceId: NamespaceID,
			ScheduleId:  ScheduleID,
		})
		return err
	})
}
