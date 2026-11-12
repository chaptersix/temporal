package scheduler_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	namespace   = schedulertest.Namespace
	namespaceID = schedulertest.NamespaceID
	scheduleID  = schedulertest.ScheduleID

	defaultInterval      = schedulertest.DefaultInterval
	defaultCatchupWindow = schedulertest.DefaultCatchupWindow
)

// newLegacySpecBuilder builds a legacy SpecBuilder with the given warn/max compute-limit bounds.
// A value of 0 means "use the default" (GetNextTime treats a non-positive bound as its default).
func newLegacySpecBuilder(warnIter, maxIter int) *legacyscheduler.SpecBuilder {
	return schedulertest.NewSpecBuilder(warnIter, maxIter)
}

// defaultSchedule returns a protobuf definition for a schedule matching this
// package's other testing defaults.
func defaultSchedule() *schedulepb.Schedule {
	return schedulertest.DefaultSchedule()
}

// defaultConfig is the shared default config with the production-style jittered
// exponential retry policy. The lifecycle harness swaps in a deterministic one
// so that rapid replays a failure identically; these tests never needed that.
func defaultConfig() *scheduler.Config {
	cfg := schedulertest.DefaultConfig()
	cfg.RetryPolicy = func() backoff.RetryPolicy {
		return backoff.NewExponentialRetryPolicy(1 * time.Second)
	}
	return cfg
}

func newTestLibrary(logger log.Logger, specProcessor scheduler.SpecProcessor) *scheduler.Library {
	return schedulertest.NewTestLibrary(schedulertest.LibraryOptions{
		Config:        defaultConfig(),
		Logger:        logger,
		SpecProcessor: specProcessor,
		SpecBuilder:   newLegacySpecBuilder(0, 0),
	})
}

// testEnv holds all components needed for scheduler tests.
type testEnv struct {
	t             *testing.T
	Ctrl          *gomock.Controller
	Registry      *chasm.Registry
	Node          *chasm.Node
	NodeBackend   *chasm.MockNodeBackend
	TimeSource    *clock.EventTimeSource
	Scheduler     *scheduler.Scheduler
	SpecProcessor scheduler.SpecProcessor
	MockEngine    *chasm.MockEngine
	Logger        log.Logger

	// allowStuckReason, when non-empty, suppresses the stuckness invariant
	// asserted by CloseTransaction. See AllowStuck.
	allowStuckReason string
}

// testEnvConfig holds configuration options for testEnv.
type testEnvConfig struct {
	specProcessor  scheduler.SpecProcessor
	withMockEngine bool
	schedule       *schedulepb.Schedule
}

// testEnvOption is a functional option for configuring testEnv.
type testEnvOption func(*testEnvConfig)

// withSpecProcessor configures testEnv with a custom SpecProcessor.
// By default, testEnv uses a real SpecProcessor. Use this option only
// when you need to mock specific SpecProcessor behavior (e.g., simulating failures).
func withSpecProcessor(sp scheduler.SpecProcessor) testEnvOption {
	return func(c *testEnvConfig) {
		c.specProcessor = sp
	}
}

// withMockEngine configures testEnv to include a mock CHASM engine for side-effect tasks.
func withMockEngine() testEnvOption {
	return func(c *testEnvConfig) {
		c.withMockEngine = true
	}
}

// withSchedule overrides defaultSchedule(), for tests that need a spec other
// than the package default 1-minute interval (e.g. an exhausted spec, which is
// what drives the Generator into its idle branch).
func withSchedule(schedule *schedulepb.Schedule) testEnvOption {
	return func(c *testEnvConfig) {
		c.schedule = schedule
	}
}

// expiredSchedule returns a schedule whose spec has already ended, so the
// Generator finds no next wakeup and takes its idle branch. This is the shape
// of a real schedule that has run to the end of its subscription window.
func expiredSchedule(now time.Time) *schedulepb.Schedule {
	schedule := defaultSchedule()
	schedule.Spec.StartTime = timestamppb.New(now.Add(-2 * time.Hour))
	schedule.Spec.EndTime = timestamppb.New(now.Add(-1 * time.Hour))
	return schedule
}

// newRealSpecProcessor creates a real SpecProcessor for tests.
func newRealSpecProcessor(ctrl *gomock.Controller, logger log.Logger) scheduler.SpecProcessor {
	return schedulertest.NewSpecProcessor(ctrl, logger, newLegacySpecBuilder(0, 0))
}

// engineTestConfig holds configuration options for newTestEngineContext.
type engineTestConfig struct {
	specProcessor scheduler.SpecProcessor
	timeSource    *clock.EventTimeSource
	engineOpts    []chasmtest.EngineOption
}

// engineTestOption is a functional option for configuring newTestEngineContext.
type engineTestOption func(*engineTestConfig)

// withEngineSpecProcessor configures newTestEngineContext with a custom
// SpecProcessor, instead of the default real one.
func withEngineSpecProcessor(sp scheduler.SpecProcessor) engineTestOption {
	return func(c *engineTestConfig) {
		c.specProcessor = sp
	}
}

// withEngineTimeSource configures the CHASM test engine with a controllable
// time source, for tests that need to advance time explicitly.
func withEngineTimeSource(ts *clock.EventTimeSource) engineTestOption {
	return func(c *engineTestConfig) {
		c.timeSource = ts
	}
}

func withEngineMetricsHandler(handler metrics.Handler) engineTestOption {
	return func(c *engineTestConfig) {
		c.engineOpts = append(c.engineOpts, chasmtest.WithMetricsHandler(handler))
	}
}

func newEngineTestConfig(opts ...engineTestOption) *engineTestConfig {
	config := &engineTestConfig{}
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// newTestEngineContext builds a CHASM registry with the core and scheduler
// libraries registered, wraps it in a chasmtest.Engine, and returns the
// engine along with an engine-bound context ready for chasm.StartExecution /
// ReadComponent / etc.
func newTestEngineContext(t *testing.T, logger log.Logger, opts ...engineTestOption) (*chasmtest.Engine, context.Context) {
	return newTestEngineContextFromConfig(t, logger, newEngineTestConfig(opts...))
}

func newTestEngineContextFromConfig(
	t *testing.T,
	logger log.Logger,
	config *engineTestConfig,
) (*chasmtest.Engine, context.Context) {
	base := newBaseEngine(t, logger, config)
	return base.Engine, base.Ctx
}

// newBaseEngine builds the shared scheduler engine with this package's config,
// spec builder and stuckness invariants.
func newBaseEngine(t *testing.T, logger log.Logger, config *engineTestConfig) *schedulertest.SchedulerEngine {
	opts := []schedulertest.Option{
		schedulertest.WithLogger(logger),
		schedulertest.WithSchedulerConfig(defaultConfig()),
		schedulertest.WithSpecBuilder(newLegacySpecBuilder(0, 0)),
		schedulertest.WithEngineOptions(config.engineOpts...),
		schedulertest.WithSchedulerInvariant(func(t chasmtest.TB, registry *chasm.Registry, node *chasm.Node, sched *scheduler.Scheduler) {
			requireNotStuck(t, node, sched)
			requireIdleCloseTimeBacked(t, registry, node, sched)
		}),
	}
	if config.specProcessor != nil {
		opts = append(opts, schedulertest.WithSpecProcessor(config.specProcessor))
	}
	if config.timeSource != nil {
		opts = append(opts, schedulertest.WithTimeSource(config.timeSource))
	}
	return schedulertest.NewEngine(t, opts...)
}

// schedulerTestEngine bundles a CHASM test engine, a created Scheduler root
// component, and convenience accessors, for tests that drive a Scheduler
// through its actual task handlers rather than the newTestEnv rapid harness.
type schedulerTestEngine struct {
	base       *schedulertest.SchedulerEngine
	engine     *chasmtest.Engine
	engineCtx  context.Context
	rootRef    chasm.ComponentRef
	logger     log.Logger
	timeSource *clock.EventTimeSource
}

// newSchedulerTestEngine builds a schedulerTestEngine and creates a schedule
// on it via the real CreateSchedule handler path. If no time source is
// supplied via withEngineTimeSource, a controllable one is created and
// wired in automatically.
func newSchedulerTestEngine(
	t *testing.T,
	schedule *schedulepb.Schedule,
	opts ...engineTestOption,
) *schedulerTestEngine {
	t.Helper()

	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	config := newEngineTestConfig(opts...)
	if config.timeSource == nil {
		config.timeSource = clock.NewEventTimeSource()
		config.timeSource.Update(time.Now())
	}
	base := newBaseEngine(t, logger, config)
	_, err := scheduler.NewTestHandler(logger).CreateSchedule(base.Ctx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   schedule,
			RequestId:  "create-request",
		},
	})
	require.NoError(t, err)
	return &schedulerTestEngine{
		base:       base,
		engine:     base.Engine,
		engineCtx:  base.Ctx,
		rootRef:    base.Ref,
		logger:     logger,
		timeSource: config.timeSource,
	}
}

// updateScheduler runs update against the Scheduler root component through
// the engine's UpdateComponent path.
func (e *schedulerTestEngine) updateScheduler(
	update func(*scheduler.Scheduler, chasm.MutableContext) error,
) error {
	return e.base.Update(update)
}

// readScheduler runs read against the Scheduler root component through the
// engine's read-only ReadComponent path.
func (e *schedulerTestEngine) readScheduler(
	read func(*scheduler.Scheduler, chasm.Context) error,
) error {
	return e.base.Read(read)
}

// newTestEnv creates a new test environment with the given options.
func newTestEnv(t *testing.T, opts ...testEnvOption) *testEnv {
	config := &testEnvConfig{}
	for _, opt := range opts {
		opt(config)
	}

	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	nodePathEncoder := chasm.DefaultPathEncoder

	// Configure spec processor: use custom if provided, otherwise use real.
	var specProcessor scheduler.SpecProcessor
	if config.specProcessor != nil {
		specProcessor = config.specProcessor
	} else {
		specProcessor = newRealSpecProcessor(ctrl, logger)
	}

	registry := chasm.NewRegistry(logger)
	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	if err := registry.Register(newTestLibrary(logger, specProcessor)); err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	timeSource := clock.NewEventTimeSource()
	now := time.Now()
	timeSource.Update(now)

	schedule := config.schedule
	if schedule == nil {
		schedule = defaultSchedule()
	}

	tv := testvars.New(t)
	nodeBackend := &chasm.MockNodeBackend{
		HandleNow:                 timeSource.Now,
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      tv.Any().WorkflowKey,
		HandleIsWorkflow:          func() bool { return false },
		HandleGetNamespaceEntry:   tv.Namespace,
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			}
		},
	}

	node := chasm.NewEmptyTree(registry, nodeBackend, nodePathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(context.Background(), node)
	sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, schedule, nil)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}
	if err = node.SetRootComponent(sched); err != nil {
		t.Fatalf("failed to set root component: %v", err)
	}

	// Advance Generator's high water mark to 'now'.
	generator := sched.Generator.Get(ctx)
	generator.LastProcessedTime = timestamppb.New(now)

	_, err = node.CloseTransaction(context.Background(), true)
	if err != nil {
		t.Fatalf("failed to close initial transaction: %v", err)
	}

	env := &testEnv{
		t:             t,
		Ctrl:          ctrl,
		Registry:      registry,
		Node:          node,
		NodeBackend:   nodeBackend,
		TimeSource:    timeSource,
		Scheduler:     sched,
		SpecProcessor: specProcessor,
		Logger:        logger,
	}

	if config.withMockEngine {
		env.MockEngine = chasm.NewMockEngine(ctrl)
	}

	return env
}

// MutableContext returns a new mutable CHASM context.
func (e *testEnv) MutableContext() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), e.Node)
}

// ReadContext returns a new read-only CHASM context.
func (e *testEnv) ReadContext() chasm.Context {
	return chasm.NewContext(context.Background(), e.Node)
}

// CloseTransaction closes the current CHASM transaction and then asserts the
// stuckness invariant: a scheduler that is not in a terminal state must carry
// at least one live logical task, or nothing will ever wake it again.
//
// This runs on every test in the package that closes through testEnv, so the
// existing suite doubles as a stuckness detector at no extra cost. Tests that
// deliberately construct a state with no pending work must opt out explicitly
// via AllowStuck.
//
// The invariant is checked only on a successful close, since Node.Snapshot
// requires a clean tree.
func (e *testEnv) CloseTransaction() error {
	e.t.Helper()

	_, err := e.Node.CloseTransaction(context.Background(), true)
	if err != nil {
		return err
	}
	if e.allowStuckReason == "" {
		requireNotStuck(e.t, e.Node, e.Scheduler)
		requireIdleCloseTimeBacked(e.t, e.Registry, e.Node, e.Scheduler)
	}
	return nil
}

// AllowStuck opts this test out of the stuckness invariant asserted by
// CloseTransaction, for tests that deliberately drive the scheduler into a
// state with no pending work.
//
// reason is mandatory: an unexplained opt-out is indistinguishable from a
// silently tolerated bug, and the set of tests that need one is itself a
// finding worth reviewing.
func (e *testEnv) AllowStuck(reason string) {
	e.t.Helper()
	if reason == "" {
		e.t.Fatal("AllowStuck requires a reason explaining why this test tolerates a stuck scheduler")
	}
	e.allowStuckReason = reason
}

// HasTask returns true if the given task type was added with the given visibilityTime.
func (e *testEnv) HasTask(task any, visibilityTime time.Time) bool {
	taskType := reflect.TypeOf(task)
	for _, categoryTasks := range e.NodeBackend.TasksByCategory {
		for _, t := range categoryTasks {
			if reflect.TypeOf(t) == taskType &&
				t.GetVisibilityTime().Equal(visibilityTime) {
				return true
			}
		}
	}
	return false
}

// HasTaskInCategory is like HasTask but scoped to a single queue category, to
// distinguish tasks that share a physical type but land in different queues.
func (e *testEnv) HasTaskInCategory(task any, category tasks.Category, visibilityTime time.Time) bool {
	taskType := reflect.TypeOf(task)
	for _, t := range e.NodeBackend.TasksByCategory[category] {
		if reflect.TypeOf(t) == taskType &&
			t.GetVisibilityTime().Equal(visibilityTime) {
			return true
		}
	}
	return false
}

// EngineContext returns a context with a mock engine. Requires withMockEngine().
func (e *testEnv) EngineContext() context.Context {
	if e.MockEngine == nil {
		e.t.Fatal("EngineContext requires withMockEngine() option")
	}
	return chasm.NewEngineContext(context.Background(), e.MockEngine)
}

// ExpectReadComponent sets up mock expectations for reading a component.
func (e *testEnv) ExpectReadComponent(ctx chasm.Context, returnedComponent chasm.Component) {
	if e.MockEngine == nil {
		e.t.Fatal("ExpectReadComponent requires withMockEngine() option")
	}
	e.MockEngine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, readFn func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
			return readFn(ctx, returnedComponent)
		}).Times(1)
}

// ExpectUpdateComponent sets up mock expectations for updating a component.
func (e *testEnv) ExpectUpdateComponent(ctx chasm.MutableContext, componentToUpdate chasm.Component) {
	if e.MockEngine == nil {
		e.t.Fatal("ExpectUpdateComponent requires withMockEngine() option")
	}
	e.MockEngine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
			err := updateFn(ctx, componentToUpdate)
			return nil, err
		}).Times(1)
}

type testInfra struct {
	node        *chasm.Node
	nodeBackend *chasm.MockNodeBackend
	logger      log.Logger
}

// setupTestInfra creates the common test infrastructure for scheduler tests.
func setupTestInfra(t *testing.T, specProcessor scheduler.SpecProcessor) *testInfra {
	nodeBackend := &chasm.MockNodeBackend{}
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	nodePathEncoder := chasm.DefaultPathEncoder

	registry := chasm.NewRegistry(logger)
	err := registry.Register(&chasm.CoreLibrary{})
	if err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	err = registry.Register(newTestLibrary(logger, specProcessor))
	if err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	tv := testvars.New(t)
	nodeBackend.HandleNow = timeSource.Now
	nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }
	nodeBackend.HandleGetWorkflowKey = tv.Any().WorkflowKey
	nodeBackend.HandleIsWorkflow = func() bool { return false }
	nodeBackend.HandleGetNamespaceEntry = tv.Namespace
	nodeBackend.HandleCurrentVersionedTransition = func() *persistencespb.VersionedTransition {
		return &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          1,
		}
	}

	node := chasm.NewEmptyTree(registry, nodeBackend, nodePathEncoder, logger, metrics.NoopMetricsHandler)
	return &testInfra{
		node:        node,
		nodeBackend: nodeBackend,
		logger:      logger,
	}
}

func setupSchedulerForTest(t *testing.T) (*scheduler.Scheduler, chasm.MutableContext, *chasm.Node) {
	ctrl := gomock.NewController(t)
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)
	specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(&scheduler.ProcessedTimeRange{
		NextWakeupTime: time.Now().Add(time.Hour),
		LastActionTime: time.Now(),
	}, nil).AnyTimes()
	specProcessor.EXPECT().NextTime(gomock.Any(), gomock.Any()).Return(legacyscheduler.GetNextTimeResult{
		Next:    time.Now().Add(time.Hour),
		Nominal: time.Now().Add(time.Hour),
	}, nil).AnyTimes()

	infra := setupTestInfra(t, specProcessor)
	ctx := chasm.NewMutableContext(context.Background(), infra.node)
	sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}
	err = infra.node.SetRootComponent(sched)
	if err != nil {
		t.Fatalf("failed to set root component: %v", err)
	}
	_, err = infra.node.CloseTransaction(context.Background(), true)
	if err != nil {
		t.Fatalf("failed to close initial transaction: %v", err)
	}

	ctx = chasm.NewMutableContext(context.Background(), infra.node)
	return sched, ctx, infra.node
}

func setupSentinelForTest(t *testing.T) (*scheduler.Scheduler, chasm.MutableContext, *chasm.Node) {
	ctrl := gomock.NewController(t)
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)

	infra := setupTestInfra(t, specProcessor)
	ctx := chasm.NewMutableContext(context.Background(), infra.node)
	sentinel := scheduler.NewSentinel(ctx, namespace, namespaceID, scheduleID)
	err := infra.node.SetRootComponent(sentinel)
	if err != nil {
		t.Fatalf("failed to set root component: %v", err)
	}
	_, err = infra.node.CloseTransaction(context.Background(), true)
	if err != nil {
		t.Fatalf("failed to close initial transaction: %v", err)
	}

	ctx = chasm.NewMutableContext(context.Background(), infra.node)
	return sentinel, ctx, infra.node
}
