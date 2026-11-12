package chasmtest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var runnerStart = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

type runnerFixture struct {
	engine *chasmtest.Engine
	ref    chasm.ComponentRef
	ts     *clock.EventTimeSource
}

// newRunnerFixture starts a payload store holding payload "ttl", whose pure
// task removes it after ttl.
func newRunnerFixture(t *testing.T, ttl time.Duration) *runnerFixture {
	t.Helper()
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(tests.Library))
	ts := clock.NewEventTimeSource()
	ts.Update(runnerStart)
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))

	key := chasm.ExecutionKey{NamespaceID: "test-ns", BusinessID: "store"}
	_, err := chasm.StartExecution(engineContext(engine), key,
		func(mc chasm.MutableContext, _ any) (*tests.PayloadStore, error) {
			store, err := tests.NewPayloadStore(mc)
			if err != nil {
				return nil, err
			}
			return store, addPayload(store, mc, "ttl", ttl)
		}, nil)
	require.NoError(t, err)
	return &runnerFixture{engine: engine, ref: chasm.NewComponentRef[*tests.PayloadStore](key), ts: ts}
}

// addSideEffect schedules a side-effect task for key at the given offset from
// the start. When present is false the key has no payload, so the task is valid
// but its execution fails with NotFound.
func (f *runnerFixture) addSideEffect(t *testing.T, key string, after time.Duration, present bool) {
	t.Helper()
	_, err := f.engine.UpdateComponent(engineContext(f.engine), f.ref,
		func(mc chasm.MutableContext, c chasm.Component) error {
			store := c.(*tests.PayloadStore)
			if present {
				if err := addPayload(store, mc, key, 0); err != nil {
					return err
				}
			}
			at := runnerStart.Add(after)
			if store.State.ExpirationTimes == nil {
				store.State.ExpirationTimes = make(map[string]*timestamppb.Timestamp)
			}
			store.State.ExpirationTimes[key] = timestamppb.New(at)
			mc.AddTask(store, chasm.TaskAttributes{ScheduledTime: at}, &testspb.TestPayloadTTLSideEffectTask{PayloadKey: key})
			return nil
		})
	require.NoError(t, err)
}

func (f *runnerFixture) hasPayload(t *testing.T, key string) bool {
	t.Helper()
	var ok bool
	require.NoError(t, f.engine.ReadComponent(engineContext(f.engine), f.ref,
		func(_ chasm.Context, c chasm.Component) error {
			_, ok = c.(*tests.PayloadStore).Payloads[key]
			return nil
		}))
	return ok
}

func TestTaskRunnerStepAdvancesClockAndFiresPureTasks(t *testing.T) {
	f := newRunnerFixture(t, time.Hour)
	r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts)

	require.True(t, r.HasPendingTask())
	require.True(t, f.hasPayload(t, "ttl"))

	require.True(t, r.Step())
	require.Equal(t, runnerStart.Add(time.Hour), f.ts.Now())
	require.False(t, f.hasPayload(t, "ttl"))

	require.False(t, r.HasPendingTask())
	require.False(t, r.Step())
	require.Equal(t, runnerStart.Add(time.Hour), f.ts.Now(), "a quiescent step leaves the clock alone")
}

func TestTaskRunnerRunToQuiescence(t *testing.T) {
	f := newRunnerFixture(t, time.Hour)
	f.addSideEffect(t, "effect", 2*time.Hour, true)
	r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts)

	steps, quiescent := r.RunToQuiescence(10)
	require.True(t, quiescent)
	require.Equal(t, 2, steps)
	require.False(t, f.hasPayload(t, "effect"))

	f = newRunnerFixture(t, time.Hour)
	f.addSideEffect(t, "effect", 2*time.Hour, true)
	r = chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts)
	steps, quiescent = r.RunToQuiescence(1)
	require.False(t, quiescent)
	require.Equal(t, 1, steps)
}

func TestTaskRunnerPendingPureTasksAreEarliestFirst(t *testing.T) {
	f := newRunnerFixture(t, time.Hour)
	_, err := f.engine.UpdateComponent(engineContext(f.engine), f.ref,
		func(mc chasm.MutableContext, c chasm.Component) error {
			return addPayload(c.(*tests.PayloadStore), mc, "sooner", time.Minute)
		})
	require.NoError(t, err)
	r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts)

	var scheduled []time.Time
	for _, task := range r.PendingPureTasks() {
		scheduled = append(scheduled, task.GetScheduledTime().AsTime())
	}
	require.Equal(t, []time.Time{runnerStart.Add(time.Minute), runnerStart.Add(time.Hour)}, scheduled)
}

func TestTaskRunnerStepPureLeavesSideEffectsUndispatched(t *testing.T) {
	for _, tc := range []struct {
		name         string
		lag          time.Duration
		wantStepPure bool
	}{
		{name: "unbounded lag", lag: 0, wantStepPure: true},
		{name: "lag shorter than the gap", lag: 5 * time.Minute, wantStepPure: false},
		{name: "lag longer than the gap", lag: 2 * time.Hour, wantStepPure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRunnerFixture(t, time.Hour)
			f.addSideEffect(t, "effect", 10*time.Minute, true)
			r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts, chasmtest.WithMaxSideEffectLag(tc.lag))

			require.Equal(t, tc.wantStepPure, r.StepPure())
			if tc.wantStepPure {
				require.Equal(t, runnerStart.Add(time.Hour), f.ts.Now())
				require.False(t, f.hasPayload(t, "ttl"))
			} else {
				require.Equal(t, runnerStart, f.ts.Now())
			}
			require.True(t, f.hasPayload(t, "effect"), "StepPure never dispatches side-effect tasks")
		})
	}
}

func TestTaskRunnerStepSideEffectDispatchesOnlyDueTasks(t *testing.T) {
	f := newRunnerFixture(t, 0)
	f.addSideEffect(t, "effect", 10*time.Minute, true)
	r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts)

	require.False(t, r.StepSideEffect(), "not due yet")
	require.True(t, f.hasPayload(t, "effect"))

	f.ts.Update(runnerStart.Add(10 * time.Minute))
	require.True(t, r.StepSideEffect())
	require.False(t, f.hasPayload(t, "effect"))
	require.False(t, r.StepSideEffect(), "a dispatched task is not dispatched again")
}

func TestTaskRunnerHookOrder(t *testing.T) {
	f := newRunnerFixture(t, time.Hour)
	f.addSideEffect(t, "effect", time.Hour, true)

	var events []string
	record := func(name string) func() { return func() { events = append(events, name) } }
	var advanceTo, clockAtAdvance time.Time
	r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts, chasmtest.WithTaskRunnerHooks(chasmtest.TaskRunnerHooks{
		BeforeAdvance: func(to time.Time) {
			advanceTo, clockAtAdvance = to, f.ts.Now()
			events = append(events, "beforeAdvance")
		},
		BeforeSideEffect:      record("beforeSideEffect"),
		AfterSideEffect:       record("afterSideEffect"),
		AfterSideEffectCommit: record("afterSideEffectCommit"),
		AfterStep:             record("afterStep"),
	}))

	require.True(t, r.Step())
	require.Equal(t, runnerStart.Add(time.Hour), advanceTo)
	require.Equal(t, runnerStart, clockAtAdvance, "BeforeAdvance runs before the clock moves")
	require.Equal(t, []string{
		"beforeAdvance",
		"afterStep", // pure tasks
		"beforeSideEffect", "afterSideEffect", "afterSideEffectCommit", "afterStep",
	}, events)
}

func TestTaskRunnerBeforeAdvanceMayMoveClock(t *testing.T) {
	f := newRunnerFixture(t, time.Hour)
	var removedBeforeMove bool
	r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts, chasmtest.WithTaskRunnerHooks(chasmtest.TaskRunnerHooks{
		BeforeAdvance: func(to time.Time) {
			f.ts.Update(to.Add(-30 * time.Minute))
			removedBeforeMove = !f.hasPayload(t, "ttl")
		},
	}))
	require.True(t, r.Step())
	require.False(t, removedBeforeMove)
	require.Equal(t, runnerStart.Add(time.Hour), f.ts.Now(), "the runner still lands on the task's time")
}

func TestTaskRunnerToleratedSideEffectError(t *testing.T) {
	f := newRunnerFixture(t, 0)
	f.addSideEffect(t, "ghost", time.Minute, false)
	f.ts.Update(runnerStart.Add(time.Minute))

	var tolerated error
	var afterSteps int
	r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts, chasmtest.WithTaskRunnerHooks(chasmtest.TaskRunnerHooks{
		TolerateSideEffectError: func(err error) bool {
			tolerated = err
			_, ok := errors.AsType[*serviceerror.NotFound](err)
			return ok
		},
		AfterStep: func() { afterSteps++ },
	}))

	require.True(t, r.StepSideEffect())
	require.Error(t, tolerated)
	require.Equal(t, 1, afterSteps, "a tolerated error still settles the step")
}

func TestTaskRunnerBeginAndFinishSideEffect(t *testing.T) {
	f := newRunnerFixture(t, 0)
	f.addSideEffect(t, "effect", time.Minute, true)

	var wrapped, afterSteps int
	r := chasmtest.NewTaskRunner(t, f.engine, f.ref, f.ts,
		chasmtest.WithAsyncEngine(func(e chasm.Engine) chasm.Engine {
			return &countingEngine{Engine: e, updates: &wrapped}
		}),
		chasmtest.WithTaskRunnerHooks(chasmtest.TaskRunnerHooks{AfterStep: func() { afterSteps++ }}),
	)

	require.Nil(t, r.SideEffectDone())
	require.False(t, r.BeginSideEffect(), "nothing is due yet")

	f.ts.Update(runnerStart.Add(time.Minute))
	require.True(t, r.BeginSideEffect())
	err := <-r.SideEffectDone()
	require.NoError(t, err)
	require.Equal(t, 0, afterSteps, "the step settles only when finished")
	r.FinishSideEffect(err)

	require.Nil(t, r.SideEffectDone())
	require.Equal(t, 1, afterSteps)
	require.Equal(t, 1, wrapped, "the handler saw the wrapped engine")
	require.False(t, f.hasPayload(t, "effect"))
	require.False(t, r.BeginSideEffect())
}

type countingEngine struct {
	chasm.Engine
	updates *int
}

func (e *countingEngine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	update func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) ([]byte, error) {
	*e.updates++
	return e.Engine.UpdateComponent(ctx, ref, update, opts...)
}
