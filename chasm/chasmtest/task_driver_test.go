package chasmtest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type driverComponent struct {
	chasm.UnimplementedComponent

	Data      *wrapperspb.Int64Value
	PureValue chasm.Field[*wrapperspb.Int64Value]
	SideValue chasm.Field[*wrapperspb.Int64Value]
}

func (c *driverComponent) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (c *driverComponent) Terminate(
	chasm.MutableContext,
	chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

func (c *driverComponent) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

type pureDriverTask struct {
	Value *wrapperspb.Int64Value
}

type sideDriverTask struct {
	Value *wrapperspb.Int64Value
}

type pureDriverTaskHandler struct {
	chasm.PureTaskHandlerBase

	err        error
	reschedule time.Duration
}

func (h *pureDriverTaskHandler) Validate(
	ctx chasm.Context,
	component *driverComponent,
	_ chasm.TaskInvocation,
	task *pureDriverTask,
) (bool, error) {
	return component.PureValue.Get(ctx).Value < task.Value.Value, nil
}

func (h *pureDriverTaskHandler) Execute(
	ctx chasm.MutableContext,
	component *driverComponent,
	attrs chasm.TaskAttributes,
	task *pureDriverTask,
) error {
	if h.err != nil {
		return h.err
	}
	component.PureValue = chasm.NewDataField(ctx, wrapperspb.Int64(task.Value.Value))
	if h.reschedule > 0 {
		ctx.AddTask(
			component,
			chasm.TaskAttributes{ScheduledTime: attrs.ScheduledTime.Add(h.reschedule)},
			&pureDriverTask{Value: wrapperspb.Int64(task.Value.Value + 1)},
		)
	}
	return nil
}

type sideDriverTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*sideDriverTask]

	err        error
	reschedule bool
}

func (h *sideDriverTaskHandler) Validate(
	ctx chasm.Context,
	component *driverComponent,
	_ chasm.TaskInvocation,
	task *sideDriverTask,
) (bool, error) {
	return component.SideValue.Get(ctx).Value < task.Value.Value, nil
}

func (h *sideDriverTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *sideDriverTask,
) error {
	if h.err != nil {
		return h.err
	}
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(component *driverComponent, mutableCtx chasm.MutableContext, task *sideDriverTask) (struct{}, error) {
			component.SideValue = chasm.NewDataField(mutableCtx, wrapperspb.Int64(task.Value.Value))
			if h.reschedule {
				mutableCtx.AddTask(
					component,
					chasm.TaskAttributes{},
					&sideDriverTask{Value: wrapperspb.Int64(task.Value.Value + 1)},
				)
			}
			return struct{}{}, nil
		},
		task,
	)
	return err
}

type driverLibrary struct {
	chasm.UnimplementedLibrary

	pureHandler *pureDriverTaskHandler
	sideHandler *sideDriverTaskHandler
}

func (l *driverLibrary) Name() string {
	return "TaskDriver"
}

func (l *driverLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*driverComponent]("Component"),
	}
}

func (l *driverLibrary) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("Pure", l.pureHandler),
		chasm.NewRegistrableSideEffectTask("Side", l.sideHandler),
	}
}

type driverTestEnv struct {
	engine      *chasmtest.Engine
	ctx         context.Context
	ref         chasm.ComponentRef
	timeSource  *clock.EventTimeSource
	pureHandler *pureDriverTaskHandler
	sideHandler *sideDriverTaskHandler
}

func TestTasksAreCopiedAndRunnableTasksAreOrdered(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "ordering")
	env.addSideTask(t, chasm.TaskAttributes{}, 1)
	env.addSideTask(t, chasm.TaskAttributes{Destination: "example.test"}, 2)
	env.addSideTask(t, chasm.TaskAttributes{ScheduledTime: now.Add(-time.Minute)}, 3)
	env.addSideTask(t, chasm.TaskAttributes{ScheduledTime: now.Add(time.Minute)}, 4)

	queued, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 3)
	require.Equal(t, tasks.CategoryTransfer, runnable[0].GetCategory())
	require.Equal(t, tasks.CategoryOutbound, runnable[1].GetCategory())
	require.Equal(t, tasks.CategoryTimer, runnable[2].GetCategory())
	require.Equal(t, now.Add(-time.Minute), runnable[2].GetVisibilityTime())

	queued[tasks.CategoryTransfer] = nil
	queuedAgain, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	require.NotEmpty(t, queuedAgain[tasks.CategoryTransfer])
}

func TestExecutePureTaskBatchesRedeliveryAndReplacement(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "pure")
	env.pureHandler.reschedule = time.Minute
	env.addPureTasks(t,
		pureTaskSpec{scheduledTime: now.Add(-time.Minute), value: 1},
		pureTaskSpec{scheduledTime: now, value: 2},
		pureTaskSpec{scheduledTime: now.Add(time.Minute), value: 3},
	)

	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	delivered := runnable[0]
	result, err := env.engine.ExecuteTask(env.ctx, env.ref, delivered)
	require.NoError(t, err)
	require.Equal(t, 2, result.LogicalTasksExecuted)
	require.Equal(t, int64(2), env.values(t).pure)

	result, err = env.engine.ExecuteTask(env.ctx, env.ref, delivered)
	require.NoError(t, err)
	require.True(t, result.Dropped)

	queued, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	require.Len(t, queued[tasks.CategoryTimer], 1)
	require.Equal(t, now.Add(time.Minute), queued[tasks.CategoryTimer][0].GetVisibilityTime())
}

func TestTaskErrorsRetryWithoutMutation(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	retryErr := errors.New("retry")

	t.Run("pure", func(t *testing.T) {
		env := newDriverTestEnv(t, now, "pure-retry")
		env.addPureTasks(t, pureTaskSpec{scheduledTime: now, value: 1})
		runnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		env.pureHandler.err = retryErr
		_, err = env.engine.ExecuteTask(env.ctx, env.ref, runnable[0])
		require.ErrorIs(t, err, retryErr)
		require.Equal(t, int64(0), env.values(t).pure)
		stillRunnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Equal(t, runnable, stillRunnable)
	})

	t.Run("side effect", func(t *testing.T) {
		env := newDriverTestEnv(t, now, "side-retry")
		env.addSideTask(t, chasm.TaskAttributes{}, 1)
		runnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		env.sideHandler.err = retryErr
		_, err = env.engine.ExecuteTask(env.ctx, env.ref, runnable[0])
		require.ErrorIs(t, err, retryErr)
		require.Equal(t, int64(0), env.values(t).side)
		stillRunnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.Equal(t, runnable, stillRunnable)
	})
}

func TestUpdateRollbackReloadAndIsolation(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "first")
	updateErr := errors.New("reject")
	_, err := env.engine.UpdateComponent(env.ctx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		driver := component.(*driverComponent)
		driver.PureValue = chasm.NewDataField(ctx, wrapperspb.Int64(10))
		ctx.AddTask(driver, chasm.TaskAttributes{}, &sideDriverTask{Value: wrapperspb.Int64(10)})
		return updateErr
	})
	require.ErrorIs(t, err, updateErr)
	require.Equal(t, driverValues{}, env.values(t))

	env.addSideTask(t, chasm.TaskAttributes{}, 2)
	queuedBefore, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	require.NoError(t, env.engine.ReloadExecution(env.ctx, env.ref))
	queuedAfter, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	require.Equal(t, queuedBefore, queuedAfter)

	otherRef := env.startExecution(t, "second")
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	_, err = env.engine.ExecuteTask(env.ctx, otherRef, runnable[0])
	require.ErrorContains(t, err, "does not match ref execution key")
}

func TestStaleTaskAndDrainLimit(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "stale")
	env.addSideTask(t, chasm.TaskAttributes{}, 1)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	env.setSideValue(t, 1)
	result, err := env.engine.ExecuteTask(env.ctx, env.ref, runnable[0])
	require.NoError(t, err)
	require.True(t, result.Dropped)

	env.sideHandler.reschedule = true
	env.addSideTask(t, chasm.TaskAttributes{}, 2)
	drained, err := env.engine.DrainTasks(env.ctx, env.ref, 3)
	require.Equal(t, 3, drained)
	require.ErrorContains(t, err, "task drain limit 3 reached")

	_, err = env.engine.DrainTasks(env.ctx, env.ref, -1)
	require.ErrorContains(t, err, "must be non-negative")
}

func TestEngineRejectsUnsupportedHistoryBehavior(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "unsupported")
	newRef := chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
		NamespaceID: "namespace-id",
		BusinessID:  "another-execution",
	}}

	newRef.RunID = "explicit-run"
	_, err := env.engine.StartExecution(env.ctx, newRef, nil, chasm.WithRequestID("request"))
	require.ErrorContains(t, err, "explicit run ID is not supported")

	_, err = env.engine.StartExecution(
		env.ctx,
		chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
			NamespaceID: "namespace-id",
			BusinessID:  "unsupported",
		}},
		nil,
		chasm.WithRequestID("replacement"),
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyTerminateExisting,
		),
	)
	require.ErrorContains(t, err, "TerminateExisting conflict policy is not supported")

	err = env.engine.ReadComponent(
		env.ctx,
		env.ref,
		nil,
		chasm.WithRefConsistencyLevel(chasm.RefConsistencyLevelCurrentRun),
	)
	require.ErrorContains(t, err, "non-default reference consistency is not supported")
}

func TestPollComponentReturnsContextCancellation(t *testing.T) {
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	env := newDriverTestEnv(t, now, "poll-cancellation")
	ctx, cancel := context.WithCancel(env.ctx)
	cancel()

	_, err := env.engine.PollComponent(ctx, env.ref, func(chasm.Context, chasm.Component) (bool, error) {
		return false, nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

type pureTaskSpec struct {
	scheduledTime time.Time
	value         int64
}

type driverValues struct {
	pure int64
	side int64
}

func newDriverTestEnv(
	t *testing.T,
	now time.Time,
	businessID string,
	opts ...chasmtest.EngineOption,
) *driverTestEnv {
	t.Helper()
	pureHandler := &pureDriverTaskHandler{}
	sideHandler := &sideDriverTaskHandler{}
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&driverLibrary{pureHandler: pureHandler, sideHandler: sideHandler}))
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	engineOpts := []chasmtest.EngineOption{
		chasmtest.WithTimeSource(timeSource),
	}
	engine := chasmtest.NewEngine(t, registry, append(engineOpts, opts...)...)
	env := &driverTestEnv{
		engine:      engine,
		ctx:         chasm.NewEngineContext(context.Background(), engine),
		timeSource:  timeSource,
		pureHandler: pureHandler,
		sideHandler: sideHandler,
	}
	env.ref = env.startExecution(t, businessID)
	return env
}

func (e *driverTestEnv) startExecution(t *testing.T, businessID string) chasm.ComponentRef {
	t.Helper()
	result, err := e.engine.StartExecution(
		e.ctx,
		chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
			NamespaceID: "namespace-id",
			BusinessID:  businessID,
		}},
		func(ctx chasm.MutableContext) (chasm.RootComponent, error) {
			return &driverComponent{
				Data:      wrapperspb.Int64(0),
				PureValue: chasm.NewDataField(ctx, wrapperspb.Int64(0)),
				SideValue: chasm.NewDataField(ctx, wrapperspb.Int64(0)),
			}, nil
		},
		chasm.WithRequestID("request-"+businessID),
	)
	require.NoError(t, err)
	ref, err := chasm.DeserializeComponentRef(result.ExecutionRef)
	require.NoError(t, err)
	return ref
}

func (e *driverTestEnv) addPureTasks(t *testing.T, specs ...pureTaskSpec) {
	t.Helper()
	e.update(t, func(ctx chasm.MutableContext, component *driverComponent) {
		for _, spec := range specs {
			ctx.AddTask(
				component,
				chasm.TaskAttributes{ScheduledTime: spec.scheduledTime},
				&pureDriverTask{Value: wrapperspb.Int64(spec.value)},
			)
		}
	})
}

func (e *driverTestEnv) addSideTask(t *testing.T, attrs chasm.TaskAttributes, value int64) {
	t.Helper()
	e.update(t, func(ctx chasm.MutableContext, component *driverComponent) {
		ctx.AddTask(component, attrs, &sideDriverTask{Value: wrapperspb.Int64(value)})
	})
}

func (e *driverTestEnv) setSideValue(t *testing.T, value int64) {
	t.Helper()
	e.update(t, func(ctx chasm.MutableContext, component *driverComponent) {
		component.SideValue = chasm.NewDataField(ctx, wrapperspb.Int64(value))
	})
}

func (e *driverTestEnv) update(t *testing.T, update func(chasm.MutableContext, *driverComponent)) {
	t.Helper()
	_, err := e.engine.UpdateComponent(e.ctx, e.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		update(ctx, component.(*driverComponent))
		return nil
	})
	require.NoError(t, err)
}

func (e *driverTestEnv) values(t *testing.T) driverValues {
	t.Helper()
	var result driverValues
	require.NoError(t, e.engine.ReadComponent(e.ctx, e.ref, func(ctx chasm.Context, component chasm.Component) error {
		driver := component.(*driverComponent)
		result = driverValues{
			pure: driver.PureValue.Get(ctx).Value,
			side: driver.SideValue.Get(ctx).Value,
		}
		return nil
	}))
	return result
}
