package rapidtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/chasmtest/rapidtest"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"pgregory.net/rapid"
)

func TestActionMapWeightsAndNames(t *testing.T) {
	actions := rapidtest.ActionMap(
		rapidtest.Action{Name: "deliver", Weight: 3, Run: func(*rapid.T) {}},
		rapidtest.Action{Name: "reload", Run: func(*rapid.T) {}},
		rapidtest.Action{Name: "", Run: func(*rapid.T) {}},
	)
	require.Len(t, actions, 5)
	require.Contains(t, actions, "deliver")
	require.Contains(t, actions, "deliver#2")
	require.Contains(t, actions, "deliver#3")
	require.Contains(t, actions, "reload")
	require.Contains(t, actions, "")
}

func TestActionMapRejectsInvalidActions(t *testing.T) {
	require.PanicsWithValue(t, "rapidtest: action \"invalid\" has negative weight -1", func() {
		rapidtest.ActionMap(rapidtest.Action{Name: "invalid", Weight: -1, Run: func(*rapid.T) {}})
	})
	require.PanicsWithValue(t, "rapidtest: invariant action must have weight one", func() {
		rapidtest.ActionMap(rapidtest.Action{Name: "", Weight: 2, Run: func(*rapid.T) {}})
	})
}

func TestExecutionActions(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
		execution, ref := newRapidExecution(rt, now)

		execution.AdvanceToNextTask(rt)
		require.Equal(rt, now.Add(time.Minute), execution.TimeSource.Now())
		execution.DeliverRunnable(rt)
		execution.Redeliver(rt)
		execution.Reload(rt)
		require.Len(rt, execution.History(), 4)

		var value int64
		require.NoError(rt, execution.Engine.ReadComponent(rt.Context(), ref, func(ctx chasm.Context, component chasm.Component) error {
			value = component.(*rapidComponent).Value.Get(ctx).Value
			return nil
		}))
		require.Equal(rt, int64(1), value)
	})
}

type rapidComponent struct {
	chasm.UnimplementedComponent

	Data  *wrapperspb.Int64Value
	Value chasm.Field[*wrapperspb.Int64Value]
}

func (c *rapidComponent) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (c *rapidComponent) Terminate(
	chasm.MutableContext,
	chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

func (c *rapidComponent) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

type rapidTask struct {
	Value *wrapperspb.Int64Value
}

type rapidTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *rapidTaskHandler) Validate(
	ctx chasm.Context,
	component *rapidComponent,
	_ chasm.TaskInvocation,
	task *rapidTask,
) (bool, error) {
	return component.Value.Get(ctx).Value < task.Value.Value, nil
}

func (h *rapidTaskHandler) Execute(
	ctx chasm.MutableContext,
	component *rapidComponent,
	_ chasm.TaskAttributes,
	task *rapidTask,
) error {
	component.Value = chasm.NewDataField(ctx, wrapperspb.Int64(task.Value.Value))
	return nil
}

type rapidLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *rapidLibrary) Name() string {
	return "RapidTest"
}

func (l *rapidLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*rapidComponent]("Component"),
	}
}

func (l *rapidLibrary) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("Task", &rapidTaskHandler{}),
	}
}

func newRapidExecution(t *rapid.T, now time.Time) (*rapidtest.Execution, chasm.ComponentRef) {
	t.Helper()
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&rapidLibrary{}))
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(timeSource))
	engineCtx := chasm.NewEngineContext(context.Background(), engine)
	result, err := engine.StartExecution(
		engineCtx,
		chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
			NamespaceID: "namespace-id",
			BusinessID:  "rapid",
			RunID:       "rapid-run",
		}},
		func(ctx chasm.MutableContext) (chasm.RootComponent, error) {
			return &rapidComponent{
				Data:  wrapperspb.Int64(0),
				Value: chasm.NewDataField(ctx, wrapperspb.Int64(0)),
			}, nil
		},
	)
	require.NoError(t, err)
	ref, err := chasm.DeserializeComponentRef(result.ExecutionRef)
	require.NoError(t, err)
	_, err = engine.UpdateComponent(engineCtx, ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		ctx.AddTask(
			component.(*rapidComponent),
			chasm.TaskAttributes{ScheduledTime: now.Add(time.Minute)},
			&rapidTask{Value: wrapperspb.Int64(1)},
		)
		return nil
	})
	require.NoError(t, err)
	return &rapidtest.Execution{
		Engine:     engine,
		Ref:        ref,
		TimeSource: timeSource,
	}, ref
}
