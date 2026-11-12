package schedulertest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// startFaults counts the upcoming StartWorkflowExecution RPCs that fail.
type startFaults struct {
	rejected  int // the RPC fails without the start being accepted
	lostReply int // the start is accepted but the response is lost
}

// Driver builds a real CHASM scheduler on top of a [chasmtest.Engine] and steps
// it forward through every task it schedules for itself, advancing a controllable
// clock. After each settled transition it invokes AfterStep (if set), which is
// where invariant assertions are plugged in.
//
// A "step", run by a [chasmtest.TaskRunner], fires every pure task due at the
// current reference time and every side-effect task whose visibility time has
// arrived, then advances the clock to the next scheduled task. A healthy interval schedule never truly quiesces (it always
// re-arms a generator task), so callers bound the run with maxSteps; quiescence is
// reached only for paused/exhausted/closed schedules.
type Driver struct {
	recordCalls        chan *RecordCall
	external           *externalLedger
	startCalls         chan *StartCall
	actionsBefore      int
	recorded           map[string]ExternalStart
	replacingSeen      map[string]struct{}
	initialActionCount int64

	T          chasmtest.TB
	Engine     *chasmtest.Engine
	Registry   *chasm.Registry
	TimeSource *clock.EventTimeSource
	Logger     log.Logger

	Frontend *workflowservicemock.MockWorkflowServiceClient
	History  *historyservicemock.MockHistoryServiceClient

	// Config is the scheduler config the library was wired with. Snapshot reads
	// Tweakables (MaxBufferSize, MaxActionsPerExecution) from it so the
	// buffer-bound and action-budget invariants can be evaluated.
	Config *scheduler.Config

	ref     chasm.ComponentRef
	execKey chasm.ExecutionKey

	// runner steps the execution through its pure and side-effect tasks.
	runner *chasmtest.TaskRunner

	// actionRPCs points at the running count of action RPCs (StartWorkflowExecution
	// + TerminateWorkflowExecution + RequestCancelWorkflowExecution) made through
	// the mock clients. It is allocated before the clients so the stub closures can
	// increment it. maxActionsObserved is the high-water mark of action RPCs made by
	// any single side-effect task dispatch (one InvokerExecuteTask), which backs the
	// action-budget invariant.
	actionRPCs         *int
	maxActionsObserved int

	// startRequests records the RequestId of every StartWorkflowExecution RPC, and
	// metricsCapture the scheduler's metrics; together they back the start ledger.
	startRequests  map[string]struct{}
	metricsCapture *metricstest.Capture
	// rpcMu guards actionRPCs and startRequests: InvokerExecuteTask issues
	// starts from concurrent goroutines.
	rpcMu *sync.Mutex

	// faults injects failures into uncontrolled start RPCs. Guarded by rpcMu.
	faults *startFaults

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
			RequestId:  "create-req",
		},
	}
	d.startExecution(func(mctx chasm.MutableContext) (*scheduler.Scheduler, error) {
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
	d.startExecution(func(mctx chasm.MutableContext) (*scheduler.Scheduler, error) {
		return scheduler.CreateSchedulerFromMigration(mctx, req)
	})
	return d
}

// newDriverScaffold builds the engine, registry, clients, and time source, but
// does not start an execution.
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

	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	ts := clock.NewEventTimeSource()
	ts.Update(cfg.startTime)

	frontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	history := historyservicemock.NewMockHistoryServiceClient(ctrl)

	// actionRPCs counts every action RPC (start/terminate/cancel) the invoker
	// makes through the mock clients, so Step can measure how many actions a single
	// ExecuteTask takes (the action-budget invariant).
	actionRPCs := new(int)
	startRequests := make(map[string]struct{})
	external := &externalLedger{starts: make(map[string]ExternalStart)}
	var startCalls chan *StartCall
	if cfg.controlledStarts {
		startCalls = make(chan *StartCall)
	}
	rpcMu := &sync.Mutex{}
	faults := &startFaults{}
	countAction := func() {
		rpcMu.Lock()
		defer rpcMu.Unlock()
		*actionRPCs++
	}

	// Default happy-path stubs; tests can layer more specific expectations.
	frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			countAction()
			rpcMu.Lock()
			startRequests[req.GetRequestId()] = struct{}{}
			rpcMu.Unlock()
			if startCalls != nil {
				call := &StartCall{Request: common.CloneProto(req), t: t, result: make(chan startResponse, 1), ledger: external}
				select {
				case startCalls <- call:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				select {
				case result := <-call.result:
					return result.response, result.err
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			rpcMu.Lock()
			rejected, lostReply := faults.rejected > 0, faults.rejected == 0 && faults.lostReply > 0
			if rejected {
				faults.rejected--
			} else if lostReply {
				faults.lostReply--
			}
			rpcMu.Unlock()
			if rejected {
				return nil, serviceerror.NewUnavailable("start rejected by driver")
			}
			accepted, err := external.accept(req)
			if err != nil {
				return nil, err
			}
			if lostReply {
				return nil, serviceerror.NewUnavailable("start accepted, response lost by driver")
			}
			return &workflowservice.StartWorkflowExecutionResponse{RunId: accepted.RunID}, nil
		}).AnyTimes()
	history.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *historyservice.TerminateWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			countAction()
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
		}).AnyTimes()
	history.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *historyservice.RequestCancelWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			countAction()
			return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
		}).AnyTimes()
	// The callbacks task (used for migrated running workflows) describes each
	// running workflow; report it completed so watchers resolve deterministically.
	history.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *historyservice.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
			return &historyservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					CloseTime: timestamppb.New(ts.Now()),
				},
			}, nil
		}).AnyTimes()

	specProcessor := NewRealSpecProcessor(ctrl, logger)

	registry := chasm.NewRegistry(logger)
	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	if err := registry.Register(NewTestLibrary(cfg.config, logger, metricsHandler, ts, specProcessor, frontend, history)); err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))

	d := &Driver{
		recordCalls: make(chan *RecordCall),
		external:    external, startCalls: startCalls, recorded: make(map[string]ExternalStart),
		replacingSeen:  make(map[string]struct{}),
		T:              t,
		Engine:         engine,
		Registry:       registry,
		TimeSource:     ts,
		Logger:         logger,
		Frontend:       frontend,
		History:        history,
		Config:         cfg.config,
		actionRPCs:     actionRPCs,
		startRequests:  startRequests,
		rpcMu:          rpcMu,
		faults:         faults,
		metricsCapture: capture,
		maxRunDuration: cfg.maxRun,
		AfterStep:      cfg.afterStep,
		execKey:        chasm.ExecutionKey{NamespaceID: NamespaceID, BusinessID: ScheduleID},
	}
	d.ref = chasm.NewComponentRef[*scheduler.Scheduler](d.execKey)
	d.runner = d.newTaskRunner(cfg.maxLag)
	return d, cfg
}

// startExecution starts the scheduler execution using the given start function.
func (d *Driver) startExecution(startFn func(chasm.MutableContext) (*scheduler.Scheduler, error)) {
	d.T.Helper()

	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)

	_, err := chasm.StartExecution(engineCtx, d.execKey, func(mctx chasm.MutableContext, _ *struct{}) (*scheduler.Scheduler, error) {
		return startFn(mctx)
	}, (*struct{})(nil), chasm.WithRequestID("create-req"))
	if err != nil {
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
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	err := d.Engine.ReadComponent(engineCtx, d.ref, func(ctx chasm.Context, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			d.T.Fatalf("root component is %T, want *scheduler.Scheduler", c)
		}
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

// newTaskRunner wires the scheduler-specific behaviour into a generic task
// runner.
func (d *Driver) newTaskRunner(maxLag time.Duration) *chasmtest.TaskRunner {
	return chasmtest.NewTaskRunner(d.T, d.Engine, d.ref, d.TimeSource,
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
			AfterStep: func() {
				if d.AfterStep != nil {
					d.AfterStep(d)
				}
			},
		}),
	)
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
		for _, start := range s.Invoker.Get(ctx).GetBufferedStarts() {
			if start.GetRunId() == "" || start.GetCompleted() != nil {
				continue
			}
			if st := start.GetStartTime().AsTime(); !have || st.Before(oldest) {
				oldest, have = st, true
			}
		}
	})
	return oldest, have
}

// CompleteWorkflow delivers a successful completion callback for the oldest
// running workflow, through Scheduler.HandleNexusCompletion. It returns false
// if no workflow is running.
func (d *Driver) CompleteWorkflow() bool {
	d.T.Helper()
	completed := false
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	_, err := d.Engine.UpdateComponent(engineCtx, d.ref, func(mctx chasm.MutableContext, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			return fmt.Errorf("root component is %T, want *scheduler.Scheduler", c)
		}
		if s.Closed || s.IsSentinel() {
			return nil
		}
		var oldest *schedulespb.BufferedStart
		for _, start := range s.Invoker.Get(mctx).GetBufferedStarts() {
			if start.GetRunId() == "" || start.GetCompleted() != nil {
				continue
			}
			if oldest == nil || start.GetStartTime().AsTime().Before(oldest.GetStartTime().AsTime()) {
				oldest = start
			}
		}
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
	if completed && d.AfterStep != nil {
		d.AfterStep(d)
	}
	return completed
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

// Patch applies a SchedulePatch through the production Scheduler.Patch path in a
// single transaction. It returns the scheduler's error (e.g. ErrClosed when the
// schedule has already closed) without failing the test, so callers driving
// random operations can tolerate rejected patches. On success the AfterStep hook
// runs so invariants are checked at the new settled state.
func (d *Driver) Patch(patch *schedulepb.SchedulePatch) error {
	d.T.Helper()
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	_, err := d.Engine.UpdateComponent(engineCtx, d.ref, func(mctx chasm.MutableContext, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			return fmt.Errorf("root component is %T, want *scheduler.Scheduler", c)
		}
		_, perr := s.Patch(mctx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: NamespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  Namespace,
				ScheduleId: ScheduleID,
				Patch:      patch,
			},
		})
		return perr
	})
	if err == nil && d.AfterStep != nil {
		d.AfterStep(d)
	}
	return err
}

// FailNextStarts makes the next n StartWorkflowExecution RPCs fail before the
// start is accepted, then lets later RPCs (the scheduler's retries) succeed.
// It does not apply under WithControlledStarts, where the test decides each
// RPC's outcome.
func (d *Driver) FailNextStarts(n int) {
	d.rpcMu.Lock()
	defer d.rpcMu.Unlock()
	d.faults.rejected += n
}

// LoseNextStartResponses makes the next n StartWorkflowExecution RPCs (after
// any FailNextStarts) accept the start but return an error, so the scheduler
// must retry with the same request ID and rely on deduplication.
func (d *Driver) LoseNextStartResponses(n int) {
	d.rpcMu.Lock()
	defer d.rpcMu.Unlock()
	d.faults.lostReply += n
}

// Backfill requests a backfill of the given range via Patch.
func (d *Driver) Backfill(req *schedulepb.BackfillRequest) error {
	return d.Patch(&schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{req}})
}

// Update applies mutate to a copy of the current schedule definition and
// replaces the schedule with it through the production Scheduler.Update path,
// without a conflict token. It returns the scheduler's error (e.g. ErrClosed)
// without failing the test. On success the AfterStep hook runs.
func (d *Driver) Update(mutate func(*schedulepb.Schedule)) error {
	d.T.Helper()
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	_, err := d.Engine.UpdateComponent(engineCtx, d.ref, func(mctx chasm.MutableContext, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			return fmt.Errorf("root component is %T, want *scheduler.Scheduler", c)
		}
		next := common.CloneProto(s.Schedule)
		mutate(next)
		_, uerr := s.Update(mctx, &schedulerpb.UpdateScheduleRequest{
			NamespaceId: NamespaceID,
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace:  Namespace,
				ScheduleId: ScheduleID,
				Schedule:   next,
			},
		})
		return uerr
	})
	if err == nil && d.AfterStep != nil {
		d.AfterStep(d)
	}
	return err
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
// StartWorkflowExecution with the exported StartScheduleArgs).
func (d *Driver) MigrateToWorkflow() error {
	d.T.Helper()
	engineCtx := chasm.NewEngineContext(context.Background(), d.Engine)
	_, err := d.Engine.UpdateComponent(engineCtx, d.ref, func(mctx chasm.MutableContext, c chasm.Component) error {
		s, ok := c.(*scheduler.Scheduler)
		if !ok {
			return fmt.Errorf("root component is %T, want *scheduler.Scheduler", c)
		}
		_, merr := s.MigrateToWorkflow(mctx, &schedulerpb.MigrateToWorkflowRequest{
			NamespaceId: NamespaceID,
			ScheduleId:  ScheduleID,
		})
		return merr
	})
	return err
}

// Snapshot captures the observable scheduler state at a settled point (after a
// step's CloseTransaction). It is the input to invariant checks.
type Snapshot struct {
	UntransferableActions int
	HasValidIdleTask      bool
	AcceptedStarts        map[string]ExternalStart
	RecordedStarts        map[string]ExternalStart
	ActionCount           int64
	InitialActionCount    int64

	// Now is the engine clock at capture time.
	Now time.Time
	// HasPendingTask is true if any task is scheduled to fire.
	HasPendingTask bool

	Closed     bool
	IsSentinel bool
	Paused     bool
	// BackfillerCount is the number of live backfiller components.
	BackfillerCount int
	// IsHeldOpen mirrors Scheduler.isHeldOpen: the schedule must stay open
	// regardless of having no work (paused or pending backfill, non-sentinel).
	IsHeldOpen bool
	// IdleCloseTime is the armed idle-close deadline, or zero if no idle task is armed.
	IdleCloseTime time.Time

	// GeneratorLPT / InvokerLPT are the generator and invoker high-water marks.
	GeneratorLPT time.Time
	InvokerLPT   time.Time

	// BufferedStartsCount is len(invoker.GetBufferedStarts()) — the total buffered
	// starts (pending + retained). MaxBufferSize is the configured cap; the
	// buffer-bound invariant requires BufferedStartsCount <= MaxBufferSize.
	BufferedStartsCount int
	MaxBufferSize       int

	// MaxActionsObserved is the high-water mark of action RPCs (start + terminate +
	// cancel) taken by any single InvokerExecuteTask so far. MaxActionsPerExecution
	// is the configured per-execution cap; the action-budget invariant requires
	// MaxActionsObserved <= MaxActionsPerExecution.
	MaxActionsObserved     int
	MaxActionsPerExecution int

	// Start ledger inputs. PendingStarts are buffered starts with no RunId yet.
	// StartRequested holds every RequestId sent to StartWorkflowExecution.
	// OverlapSkipped and DroppedStarts count the starts the scheduler discarded
	// on purpose (Info.OverlapSkipped, and the ScheduleBufferedStartDropped
	// metric for missed-catchup and paused/limited drops).
	PendingStarts  map[string]struct{}
	StartRequested map[string]struct{}
	// ReplacingStarts counts the pending starts that no start RPC has been attempted for (including deferred ones) whose overlap policy
	// is CANCEL_OTHER or TERMINATE_OTHER. With nothing running, such a start
	// replaces the one the buffer would otherwise have started
	// (legacyscheduler.ProcessBuffer), discarding it without counting an overlap
	// skip, so each can account for one silent disappearance. ReplacingSeen is
	// the number of distinct such starts ever seen in the buffer in any state,
	// which covers a replacer that was buffered and started between two snapshots.
	ReplacingStarts int
	ReplacingSeen   int
	OverlapSkipped  int64
	DroppedStarts   int64
	// Migrating is true once the schedule hands off to a V1 workflow, which
	// takes its buffered starts with it.
	Migrating bool
	// DeferredStarts are pending starts waiting on a workflow completion
	// (Attempt == -1); RunningWorkflows counts started, uncompleted workflows.
	DeferredStarts   int
	RunningWorkflows int
}

// Snapshot reads current scheduler state into a Snapshot.
func (d *Driver) Snapshot() Snapshot {
	d.T.Helper()
	d.observeRecordedStarts()
	tweakables := d.Config.Tweakables(Namespace)
	snap := Snapshot{
		AcceptedStarts: d.external.snapshot(), RecordedStarts: maps.Clone(d.recorded),
		InitialActionCount:     d.initialActionCount,
		Now:                    d.Now(),
		HasPendingTask:         d.HasPendingTask(),
		MaxBufferSize:          tweakables.MaxBufferSize,
		MaxActionsObserved:     d.maxActionsObserved,
		MaxActionsPerExecution: tweakables.MaxActionsPerExecution,
		PendingStarts:          make(map[string]struct{}),
		StartRequested:         d.startRequestsCopy(),
		DroppedStarts:          d.droppedStarts(),
	}
	d.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		snap.HasValidIdleTask = d.validIdleTask(s, ctx)
		snap.ActionCount = s.Info.GetActionCount()
		snap.Closed = s.Closed
		snap.IsSentinel = s.IsSentinel()
		snap.Paused = s.Schedule.GetState().GetPaused()
		snap.BackfillerCount = len(s.Backfillers)
		snap.OverlapSkipped = s.Info.GetOverlapSkipped()
		snap.Migrating = s.GetWorkflowMigration() != nil
		// Mirror Scheduler.isHeldOpen() from public state.
		snap.IsHeldOpen = !snap.IsSentinel && (snap.Paused || snap.BackfillerCount > 0)
		if s.IdleCloseTime != nil {
			snap.IdleCloseTime = s.IdleCloseTime.AsTime()
		}
		// Sentinels have no generator/invoker sub-components.
		if !snap.IsSentinel {
			if g := s.Generator.Get(ctx); g != nil && g.LastProcessedTime != nil {
				snap.GeneratorLPT = g.LastProcessedTime.AsTime()
			}
			if inv := s.Invoker.Get(ctx); inv != nil {
				snap.UntransferableActions = len(inv.GetCancelWorkflows()) + len(inv.GetTerminateWorkflows())
				snap.BufferedStartsCount = len(inv.GetBufferedStarts())
				snap.addBufferedStarts(inv.GetBufferedStarts(), s.Schedule.GetPolicies().GetOverlapPolicy(), d.replacingSeen)
				snap.ReplacingSeen = len(d.replacingSeen)
				if inv.LastProcessedTime != nil {
					snap.InvokerLPT = inv.LastProcessedTime.AsTime()
				}
			}
		}
	})
	return snap
}

// addBufferedStarts classifies buffered starts into pending (deferred or not)
// and running. scheduleOverlap resolves starts with an unspecified policy.
// replacingSeen accumulates the request IDs of replacing starts across snapshots.
func (snap *Snapshot) addBufferedStarts(starts []*schedulespb.BufferedStart, scheduleOverlap enumspb.ScheduleOverlapPolicy, replacingSeen map[string]struct{}) {
	for _, start := range starts {
		policy := cmp.Or(start.GetOverlapPolicy(), scheduleOverlap)
		replacing := policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER ||
			policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
		if replacing {
			replacingSeen[start.GetRequestId()] = struct{}{}
		}
		switch {
		case start.GetRunId() == "":
			if replacing && start.GetAttempt() <= 0 {
				snap.ReplacingStarts++
			}
			if start.GetAttempt() > 0 {
				snap.UntransferableActions++
			}
			snap.PendingStarts[start.GetRequestId()] = struct{}{}
			if start.GetAttempt() == -1 {
				snap.DeferredStarts++
			}
		case start.GetCompleted() == nil:
			snap.RunningWorkflows++
		default:
		}
	}
}

// droppedStarts sums the ScheduleBufferedStartDropped counter.
func (d *Driver) droppedStarts() int64 {
	var total int64
	for _, rec := range d.metricsCapture.SnapshotMetric(metrics.ScheduleBufferedStartDropped.Name()) {
		if v, ok := rec.Value.(int64); ok {
			total += v
		}
	}
	return total
}

func (d *Driver) actionCount() int {
	d.rpcMu.Lock()
	defer d.rpcMu.Unlock()
	return *d.actionRPCs
}

func (d *Driver) startRequestsCopy() map[string]struct{} {
	d.rpcMu.Lock()
	defer d.rpcMu.Unlock()
	return maps.Clone(d.startRequests)
}
