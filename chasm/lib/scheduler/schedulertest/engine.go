package schedulertest

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/testing/testlogger"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
)

// CreateRequestID is the request ID [SchedulerEngine.Start] records for the
// scheduler's creating transaction.
const CreateRequestID = "create-req"

// SchedulerEngine bundles a [chasmtest.Engine] with the CHASM registry that has
// the scheduler library registered, the reference of the Scheduler root
// component, and typed read/update helpers. It is the shared base of the unit
// tests in package scheduler_test and of [Driver].
//
// NewEngine only builds the engine. Create the execution with [SchedulerEngine.Start],
// or through a handler using [SchedulerEngine.Ctx].
type SchedulerEngine struct {
	T        chasmtest.TB
	Engine   *chasmtest.Engine
	Registry *chasm.Registry
	Logger   log.Logger
	// State is the stuck and idle-deadline invariant the engine asserts after
	// every committed transition (unless built WithoutStateCheck).
	State *StateInvariant
	// Config is the scheduler config the library was wired with.
	Config *scheduler.Config
	// TimeSource is the clock the engine was built with, or nil if the engine
	// kept its default.
	TimeSource *clock.EventTimeSource
	// SpecProcessor is the processor the library was wired with.
	SpecProcessor scheduler.SpecProcessor
	// Ctx carries the engine, ready for chasm.StartExecution, chasm.ReadComponent
	// and handler calls.
	Ctx context.Context
	Key chasm.ExecutionKey
	Ref chasm.ComponentRef

	// node and root are the tree and Scheduler as of the last committed
	// transition, for StateViolations.
	node *chasm.Node
	root *scheduler.Scheduler
}

type engineOptions struct {
	logger       log.Logger
	library      LibraryOptions
	engineOpts   []chasmtest.EngineOption
	timeSource   *clock.EventTimeSource
	noStateCheck bool
}

// Option configures [NewEngine].
type Option func(*engineOptions)

// WithLogger overrides the default test logger.
func WithLogger(l log.Logger) Option {
	return func(o *engineOptions) { o.logger = l }
}

// WithSchedulerConfig overrides the scheduler config (default: [DefaultConfig]). Use it
// to set small Tweakables, or a different retry policy.
func WithSchedulerConfig(cfg *scheduler.Config) Option {
	return func(o *engineOptions) { o.library.Config = cfg }
}

// WithSpecProcessor overrides the real spec processor, e.g. to simulate failures.
func WithSpecProcessor(sp scheduler.SpecProcessor) Option {
	return func(o *engineOptions) { o.library.SpecProcessor = sp }
}

// WithSpecBuilder overrides the spec builder used by both the library and the
// default spec processor (default: [DefaultSpecBuilder]).
func WithSpecBuilder(b *legacyscheduler.SpecBuilder) Option {
	return func(o *engineOptions) { o.library.SpecBuilder = b }
}

// WithTimeSource gives the engine a controllable clock. Without it the engine
// keeps chasmtest's default, which is frozen at time.Now.
func WithTimeSource(ts *clock.EventTimeSource) Option {
	return func(o *engineOptions) {
		o.timeSource = ts
		o.engineOpts = append(o.engineOpts, chasmtest.WithTimeSource(ts))
	}
}

// WithInvokerTimeSource also hands the clock to the invoker, which stamps
// StartTime on started workflows. Without it the invoker uses real time.
func WithInvokerTimeSource(ts clock.TimeSource) Option {
	return func(o *engineOptions) { o.library.TimeSource = ts }
}

// WithInvokerMetrics gives the invoker task handlers a metrics handler.
func WithInvokerMetrics(h metrics.Handler) Option {
	return func(o *engineOptions) { o.library.MetricsHandler = h }
}

// WithClients wires the frontend and history clients behind the side-effect
// task handlers (execute, callbacks, migrate).
func WithClients(frontend workflowservice.WorkflowServiceClient, history resource.HistoryClient) Option {
	return func(o *engineOptions) {
		o.library.FrontendClient = frontend
		o.library.HistoryClient = history
	}
}

// WithEngineOptions passes options straight to [chasmtest.NewEngine], e.g. an
// extra invariant check or an engine metrics handler.
func WithEngineOptions(opts ...chasmtest.EngineOption) Option {
	return func(o *engineOptions) { o.engineOpts = append(o.engineOpts, opts...) }
}

// WithoutStateCheck leaves the stuck and idle-deadline invariants off the
// engine. Only a test that corrupts the scheduler on purpose and asserts
// [StateInvariant.Violations] catches it should need this.
func WithoutStateCheck() Option {
	return func(o *engineOptions) { o.noStateCheck = true }
}

// NewEngine builds a registry with the core and scheduler libraries, wraps it in
// a [chasmtest.Engine], and returns the wrapper. No execution is started.
func NewEngine(t chasmtest.TB, opts ...Option) *SchedulerEngine {
	t.Helper()

	o := &engineOptions{}
	for _, opt := range opts {
		opt(o)
	}
	if o.logger == nil {
		o.logger = testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	}
	if o.library.Config == nil {
		o.library.Config = DefaultConfig()
	}
	if o.library.SpecBuilder == nil {
		o.library.SpecBuilder = DefaultSpecBuilder()
	}
	if o.library.SpecProcessor == nil {
		o.library.SpecProcessor = NewSpecProcessor(gomock.NewController(t), o.logger, o.library.SpecBuilder)
	}
	o.library.Logger = o.logger

	registry := chasm.NewRegistry(o.logger)
	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	if err := registry.Register(NewTestLibrary(o.library)); err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	state := NewStateInvariant(registry, o.library.Config, o.logger)
	se := &SchedulerEngine{
		T:             t,
		Registry:      registry,
		State:         state,
		Logger:        o.logger,
		Config:        o.library.Config,
		TimeSource:    o.timeSource,
		SpecProcessor: o.library.SpecProcessor,
	}
	engineOpts := append(o.engineOpts, chasmtest.WithInvariantCheck(
		func(t chasmtest.TB, node *chasm.Node, root chasm.RootComponent) {
			t.Helper()
			sched, ok := root.(*scheduler.Scheduler)
			if !ok {
				t.Fatalf("scheduler test engine root has unexpected type %T", root)
			}
			se.node, se.root = node, sched
			if !o.noStateCheck {
				state.Check(t, node, sched)
			}
		},
	))
	engine := chasmtest.NewEngine(t, registry, engineOpts...)

	se.Engine = engine
	se.Key = chasm.ExecutionKey{NamespaceID: NamespaceID, BusinessID: ScheduleID}
	se.Ref = chasm.NewComponentRef[*scheduler.Scheduler](se.Key)
	se.Ctx = chasm.NewEngineContext(context.Background(), engine)
	return se
}

// StateViolations evaluates the stuck and idle-deadline invariants against the
// last committed state without failing the test. It is how a test built
// [WithoutStateCheck] asserts that the check catches state it corrupted on
// purpose.
func (e *SchedulerEngine) StateViolations() []string {
	e.T.Helper()
	if e.node == nil {
		e.T.Fatalf("no transition has committed yet")
	}
	return e.State.Violations(e.node, e.root)
}

// Start starts the scheduler execution with startFn, recording [CreateRequestID].
func (e *SchedulerEngine) Start(startFn func(chasm.MutableContext) (*scheduler.Scheduler, error)) error {
	e.T.Helper()
	_, err := chasm.StartExecution(e.Ctx, e.Key, func(mctx chasm.MutableContext, _ *struct{}) (*scheduler.Scheduler, error) {
		return startFn(mctx)
	}, (*struct{})(nil), chasm.WithRequestID(CreateRequestID))
	return err
}

// Update runs update against the Scheduler root component through the engine's
// UpdateComponent path.
func (e *SchedulerEngine) Update(update func(*scheduler.Scheduler, chasm.MutableContext) error) error {
	e.T.Helper()
	_, _, err := chasm.UpdateComponent(
		e.Ctx,
		e.Ref,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			return struct{}{}, update(s, ctx)
		},
		struct{}{},
	)
	return err
}

// Read runs read against the Scheduler root component through the engine's
// read-only ReadComponent path.
func (e *SchedulerEngine) Read(read func(*scheduler.Scheduler, chasm.Context) error) error {
	e.T.Helper()
	_, err := chasm.ReadComponent(
		e.Ctx,
		e.Ref,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, read(s, ctx)
		},
		struct{}{},
	)
	return err
}
