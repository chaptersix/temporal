//go:build test_dep

package chasm

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestBoundedWorkPointMutation(t *testing.T) {
	t.Parallel()
	for _, size := range []int{4, 5, 1000, 10000} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			root, value, ctx := boundedFixtureTree(t)
			for i := range size {
				value.Records.Put(ctx, strconv.Itoa(i), &commonpb.Payload{Data: []byte("unrelated")})
			}
			require.NoError(t, root.SetRootComponent(value))
			closeDataMap(t, root)
			fresh := reloadMemoTree(t, root, memoSnapshot(root))
			counts := fresh.BeginWorkInspectionForTest()
			for step := range 2 {
				counts.Reset()
				ctx := NewMutableContext(t.Context(), fresh)
				c, err := fresh.Component(ctx, ComponentRef{})
				require.NoError(t, err)
				current := c.(*dataMapFixture)
				item, ok := current.Records.Get(ctx, "0")
				require.True(t, ok)
				item.Data = []byte(strconv.Itoa(step))
				current.Records.Put(ctx, "0", item)
				mutation := closeDataMap(t, fresh)
				require.Contains(t, mutation.UpdatedNodes, "Records#0")
				require.Empty(t, mutation.DeletedNodes)
				hot := counts.Snapshot()
				require.Zero(t, hot["tree_visits"])
				require.Zero(t, hot["map_hydration_keys"])
				require.Zero(t, hot["map_key_copies"])
				require.EqualValues(t, 1-step, hot["data_decode_calls"])
				// One root slot: four dirty notifications plus one task pass.
				require.LessOrEqual(t, hot["slot_visits"], int64(5))
				// One structure entry, two byte entries, three touched ancestors.
				require.LessOrEqual(t, hot["workset_enqueues"], int64(6))
				emitWorkCounts(t, size, "bounded-hot"+strconv.Itoa(step), hot)
			}
			require.Len(t, fresh.Snapshot(nil).Nodes, size+2)
		})
	}
}
func TestBoundedWorkRedirtyAfterDrain(t *testing.T) {
	t.Parallel()
	root, value, _ := boundedFixtureTree(t)
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	ctx := NewMutableContext(t.Context(), root)
	for _, key := range []string{"first", "second"} {
		_, err := root.Component(ctx, ComponentRef{})
		require.NoError(t, err)
		value.Records.Put(ctx, key, &commonpb.Payload{Data: []byte(key)})
		require.NoError(t, root.syncSubComponents())
	}
	mutation := closeDataMap(t, root)
	require.Contains(t, mutation.UpdatedNodes, "Records#first")
	require.Contains(t, mutation.UpdatedNodes, "Records#second")
	fresh := reloadMemoTree(t, root, memoSnapshot(root))
	c, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
	require.NoError(t, err)
	for _, key := range []string{"first", "second"} {
		got, ok := c.(*dataMapFixture).Records.Get(NewContext(t.Context(), fresh), key)
		require.True(t, ok)
		require.Equal(t, key, string(got.Data))
	}
}

type boundedTaskLibrary struct {
	UnimplementedLibrary
	limits     BoundedTaskLimits
	executions int
}

func (*boundedTaskLibrary) Name() string { return "boundedtasks" }
func (l *boundedTaskLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{NewRegistrableComponent[*dataMapFixture]("root", WithBoundedDataLayout(BoundedDataLayout{Components: []BoundedComponentSlot{BoundedComponent[*dataMapFixture](nil, l.limits)}, MaxImmediateExecutions: l.executions}))}
}
func (*boundedTaskLibrary) Tasks() []*RegistrableTask {
	return []*RegistrableTask{NewRegistrablePureTask("tick", &boundedTick{})}
}

type boundedTick struct{ PureTaskHandlerBase }

func (*boundedTick) Validate(_ Context, c *dataMapFixture, _ TaskInvocation, task *commonpb.Payload) (bool, error) {
	return c.State.RunId != "invalid" && c.State.RunId != strconv.Itoa(int(task.Data[0])), nil
}
func (*boundedTick) Execute(ctx MutableContext, c *dataMapFixture, _ TaskAttributes, task *commonpb.Payload) error {
	count := task.Data[0]
	c.State.RunId = strconv.Itoa(int(count))
	if count > 0 {
		ctx.AddTask(c, TaskAttributes{}, &commonpb.Payload{Data: []byte{count - 1}})
	}
	return nil
}
func boundedTaskTree(t *testing.T, limits BoundedTaskLimits, executions int) (*Node, *dataMapFixture, MutableContext) {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	registry := NewRegistry(logger)
	require.NoError(t, registry.Register(&CoreLibrary{}))
	require.NoError(t, registry.Register(&boundedTaskLibrary{limits: limits, executions: executions}))
	backend := &MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }}
	root := NewEmptyTree(registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	value := &dataMapFixture{State: &persistencespb.WorkflowExecutionState{}}
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	return root, value, NewMutableContext(t.Context(), root)
}
func TestBoundedTaskBudgets(t *testing.T) {
	t.Parallel()
	t.Run("immediate chain fits", func(t *testing.T) {
		t.Parallel()
		root, value, ctx := boundedTaskTree(t, BoundedTaskLimits{ImmediatePerTransaction: 3}, 3)
		ctx.AddTask(value, TaskAttributes{}, &commonpb.Payload{Data: []byte{2}})
		closeDataMap(t, root)
		require.Equal(t, "0", value.State.RunId)
		require.Zero(t, root.bounded.executed)
		require.Empty(t, root.bounded.issued)
	})
	t.Run("enqueue cap before growth", func(t *testing.T) {
		t.Parallel()
		root, value, ctx := boundedTaskTree(t, BoundedTaskLimits{}, 0)
		require.Panics(t, func() { ctx.AddTask(value, TaskAttributes{}, &commonpb.Payload{Data: []byte{0}}) })
		require.Empty(t, root.immediatePureTasks)
	})
	t.Run("execution cap spans waves", func(t *testing.T) {
		t.Parallel()
		root, value, ctx := boundedTaskTree(t, BoundedTaskLimits{ImmediatePerTransaction: 3}, 2)
		ctx.AddTask(value, TaskAttributes{}, &commonpb.Payload{Data: []byte{2}})
		mutation, err := root.CloseTransaction(t.Context(), true)
		require.ErrorContains(t, err, "execution limit")
		require.Empty(t, mutation.UpdatedNodes)
	})
	t.Run("persisted cap", func(t *testing.T) {
		t.Parallel()
		root, value, ctx := boundedTaskTree(t, BoundedTaskLimits{Pure: 1, NewPerTransaction: 2}, 0)
		_, err := root.Component(ctx, ComponentRef{})
		require.NoError(t, err)
		for range 2 {
			ctx.AddTask(value, TaskAttributes{ScheduledTime: time.Unix(2000000000, 0)}, &commonpb.Payload{Data: []byte{0}})
		}
		mutation, err := root.CloseTransaction(t.Context(), true)
		require.ErrorContains(t, err, "persisted task limit")
		require.Empty(t, mutation.UpdatedNodes)
	})
}
