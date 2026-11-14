//go:build test_dep

package chasm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

type workTaskRoot struct {
	UnimplementedComponent
	State   *commonpb.Payload
	Records DataMap[*commonpb.Payload]
	Child   Field[*workTaskChild]
	Sibling Field[*workTaskChild]
}

func (r *workTaskRoot) LifecycleState(Context) LifecycleState {
	if string(r.State.Data) == "closed" {
		return LifecycleStateCompleted
	}
	return LifecycleStateRunning
}
func (*workTaskRoot) ContextMetadata(Context) map[string]string { return nil }
func (*workTaskRoot) Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error) {
	return TerminateComponentResponse{}, nil
}

type workTaskChild struct {
	UnimplementedComponent
	State  *commonpb.Payload
	Parent ParentPtr[*workTaskRoot]
}

func (*workTaskChild) LifecycleState(Context) LifecycleState { return LifecycleStateRunning }
func (c *workTaskChild) valid(ctx Context) bool {
	root := c.Parent.Get(ctx)
	gate, ok := root.Records.Get(ctx, "gate")
	return (!ok || string(gate.Data) != "closed") && string(c.State.Data) != "ran"
}

type workTaskPure struct{ PureTaskHandlerBase }

func (*workTaskPure) Validate(ctx Context, c *workTaskChild, _ TaskInvocation, _ *commonpb.Payload) (bool, error) {
	return c.valid(ctx), nil
}
func (*workTaskPure) Execute(_ MutableContext, c *workTaskChild, _ TaskAttributes, _ *commonpb.Payload) error {
	c.State.Data = []byte("ran")
	return nil
}

type workTaskEffect struct {
	SideEffectTaskHandlerBase[*commonpb.Header]
}

func (*workTaskEffect) Validate(ctx Context, c *workTaskChild, _ TaskInvocation, _ *commonpb.Header) (bool, error) {
	return c.valid(ctx), nil
}
func (*workTaskEffect) Execute(context.Context, ComponentRef, TaskAttributes, *commonpb.Header) error {
	return nil
}

type workTaskLibrary struct {
	UnimplementedLibrary
	bounded bool
	offline bool
}

func (*workTaskLibrary) Name() string { return "worktaskfixture" }
func (l *workTaskLibrary) Components() []*RegistrableComponent {
	var options []RegistrableComponentOption
	if l.bounded {
		options = append(options, WithBoundedDataLayout(BoundedDataLayout{Components: []BoundedComponentSlot{BoundedComponent[*workTaskRoot](nil, BoundedTaskLimits{}), BoundedComponent[*workTaskChild]([]string{"Child"}, BoundedTaskLimits{Pure: 1, SideEffect: 1, NewPerTransaction: 2}), BoundedComponent[*workTaskChild]([]string{"Sibling"}, BoundedTaskLimits{Pure: 1, SideEffect: 1, NewPerTransaction: 2})}}))
	}
	return []*RegistrableComponent{NewRegistrableComponent[*workTaskRoot]("root", options...), NewRegistrableComponent[*workTaskChild]("child")}
}
func (l *workTaskLibrary) Tasks() []*RegistrableTask {
	if l.offline {
		return []*RegistrableTask{NewRegistrablePureTask[*workTaskChild, *commonpb.Payload]("pure", nil), NewRegistrableSideEffectTask[*workTaskChild, *commonpb.Header]("effect", nil)}
	}
	return []*RegistrableTask{NewRegistrablePureTask("pure", &workTaskPure{}), NewRegistrableSideEffectTask("effect", &workTaskEffect{})}
}
func newWorkTaskTree(t *testing.T, bounded, detached bool) (*Node, *workTaskRoot) {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	registry := NewRegistry(logger)
	require.NoError(t, registry.Register(&CoreLibrary{}))
	require.NoError(t, registry.Register(&workTaskLibrary{bounded: bounded}))
	execution := &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING}
	backend := &MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }, HandleNow: func() time.Time { return time.Unix(100, 0) }, HandleGetExecutionState: func() *persistencespb.WorkflowExecutionState { return execution }, HandleUpdateWorkflowStateStatus: func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
		changed := execution.State != state
		execution.State = state
		execution.Status = status
		return changed, nil
	}}
	root := NewEmptyTree(registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := NewMutableContext(t.Context(), root)
	value := &workTaskRoot{State: &commonpb.Payload{}, Records: NewDataMap[*commonpb.Payload](ctx)}
	child := &workTaskChild{State: &commonpb.Payload{}}
	var options []ComponentFieldOption
	if detached {
		options = append(options, ComponentFieldDetached())
	}
	value.Child = NewComponentField(ctx, child, options...)
	value.Records.Put(ctx, "gate", &commonpb.Payload{Data: []byte("open")})
	value.Records.Put(ctx, "poison", &commonpb.Payload{})
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	ctx = NewMutableContext(t.Context(), root)
	_, err := root.Component(ctx, ComponentRef{})
	require.NoError(t, err)
	child = value.Child.Get(ctx)
	ctx.AddTask(child, TaskAttributes{ScheduledTime: time.Unix(200, 0)}, &commonpb.Payload{})
	ctx.AddTask(child, TaskAttributes{ScheduledTime: time.Unix(300, 0)}, &commonpb.Header{})
	closeDataMap(t, root)
	// Corrupt an unrelated record. Every task consumer must leave it unread.
	root.children["Records"].children["poison"].serializedNode.Data = &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte{0xff}}
	fresh := reloadMemoTree(t, root, memoSnapshot(root))
	c, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
	require.NoError(t, err)
	return fresh, c.(*workTaskRoot)
}
func compareWorkTaskTrees(t *testing.T, left, right *Node) {
	t.Helper()
	a, b := memoSnapshot(left), memoSnapshot(right)
	require.Len(t, b, len(a))
	for path, node := range a {
		require.True(t, proto.Equal(node, b[path]), "node %s differs", path)
	}
	require.Equal(t, left.backend.(*MockNodeBackend).TasksByCategory, right.backend.(*MockNodeBackend).TasksByCategory)
}
func TestBoundedTasksMatchDefaultConsumers(t *testing.T) {
	t.Parallel()
	raw, _ := newWorkTaskTree(t, false, false)
	bounded, _ := newWorkTaskTree(t, true, false)
	compareWorkTaskTrees(t, raw, bounded)
	require.Equal(t, raw.defaultFindNextTargetTime(), bounded.defaultFindNextTargetTime())
	for _, root := range []*Node{raw, bounded} {
		require.NoError(t, root.RefreshTasks())
		closeDataMap(t, root)
		require.NoError(t, root.regenerateTimerTasksForTimeSkipping())
		closeDataMap(t, root)
	}
	compareWorkTaskTrees(t, raw, bounded)
	for _, root := range []*Node{raw, bounded} {
		executions := 0
		require.NoError(t, root.EachPureTask(time.Unix(200, 0), func(handler NodePureTask, attrs TaskAttributes, payload any) (bool, error) {
			ran, err := handler.ExecutePureTask(t.Context(), attrs, payload)
			if ran {
				executions++
			}
			return ran, err
		}))
		require.Equal(t, 1, executions)
		closeDataMap(t, root)
		// A duplicate logical delivery must be invalidated after the successful pure task.
		executed, err := root.children["Child"].ExecutePureTask(t.Context(), TaskAttributes{ScheduledTime: time.Unix(200, 0)}, &commonpb.Payload{})
		require.NoError(t, err)
		require.False(t, executed)
	}
	compareWorkTaskTrees(t, raw, bounded)
}
func TestBoundedTasksCrossSlotInvalidationAndLifecycle(t *testing.T) {
	t.Parallel()
	for _, detached := range []bool{false, true} {
		t.Run(map[bool]string{false: "attached", true: "detached"}[detached], func(t *testing.T) {
			t.Parallel()
			raw, left := newWorkTaskTree(t, false, detached)
			bounded, right := newWorkTaskTree(t, true, detached)
			for i, root := range []*Node{raw, bounded} {
				value := []*workTaskRoot{left, right}[i]
				// Only a DATA leaf changes. The other fixed component's ParentPtr validator
				// reads this leaf and must invalidate both its persisted tasks.
				value.Records.Put(NewMutableContext(t.Context(), root), "gate", &commonpb.Payload{Data: []byte("closed")})
				closeDataMap(t, root)
				attrs := root.children["Child"].serializedNode.Metadata.GetComponentAttributes()
				require.Empty(t, attrs.PureTasks)
				require.Empty(t, attrs.SideEffectTasks)
			}
			compareWorkTaskTrees(t, raw, bounded)
			raw, left = newWorkTaskTree(t, false, detached)
			bounded, right = newWorkTaskTree(t, true, detached)
			for i, root := range []*Node{raw, bounded} {
				_, err := root.Component(NewMutableContext(t.Context(), root), ComponentRef{})
				require.NoError(t, err)
				[]*workTaskRoot{left, right}[i].State.Data = []byte("closed")
				closeDataMap(t, root)
				attrs := root.children["Child"].serializedNode.Metadata.GetComponentAttributes()
				require.Equal(t, detached, len(attrs.PureTasks) == 1)
				require.Equal(t, detached, len(attrs.SideEffectTasks) == 1)
			}
			compareWorkTaskTrees(t, raw, bounded)
		})
	}
}
func TestBoundedWorkFailureReloadPreservesTasks(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"serializer", "structure"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root, value := newWorkTaskTree(t, true, false)
			saved := memoSnapshot(root)
			ctx := NewMutableContext(t.Context(), root)
			_, err := root.Component(ctx, ComponentRef{})
			require.NoError(t, err)
			value.Records.Put(ctx, "gate", &commonpb.Payload{Data: []byte("closed")})
			switch kind {
			case "serializer":
				value.Child.Get(ctx).State.Metadata = map[string][]byte{string([]byte{0xff}): {1}}
			case "structure":
				value.Child = ComponentPointerTo(ctx, value.Child.Get(ctx))
			default:
				t.Fatal("unexpected failure")
			}
			mutation, err := root.CloseTransaction(t.Context(), true)
			require.Error(t, err)
			require.Empty(t, mutation.UpdatedNodes)
			fresh := reloadMemoTree(t, root, saved)
			require.Len(t, fresh.children["Child"].serializedNode.Metadata.GetComponentAttributes().PureTasks, 1)
			require.Len(t, fresh.children["Child"].serializedNode.Metadata.GetComponentAttributes().SideEffectTasks, 1)
			c, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
			require.NoError(t, err)
			gate, ok := c.(*workTaskRoot).Records.Get(NewContext(t.Context(), fresh), "gate")
			require.True(t, ok)
			require.Equal(t, "open", string(gate.Data))
			require.Empty(t, fresh.bounded.bytes.entries)
			require.Empty(t, fresh.bounded.structure.entries)
		})
	}
}

func TestBoundedWorkOfflineAndCompleteExport(t *testing.T) {
	t.Parallel()
	root, _ := newWorkTaskTree(t, true, false)
	all := memoSnapshot(root)
	partition, local := root.PartitionedSnapshot(nil)
	require.Len(t, partition.Nodes, len(all))
	require.Contains(t, local.Nodes, "Child")
	require.True(t, proto.Equal(all["Records#poison"], partition.Nodes["Records#poison"]))
	require.True(t, proto.Equal(all["Child"], root.children["Child"].serializedNode), "partition must not mutate live task status")
	registry := NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	require.NoError(t, registry.Register(&CoreLibrary{}))
	require.NoError(t, registry.Register(&workTaskLibrary{bounded: true, offline: true}))
	record := &persistencespb.WorkflowMutableState{ChasmNodes: all, ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionState: &persistencespb.WorkflowExecutionState{}}
	value, ctx, err := NewDetachedExecution[*workTaskRoot](t.Context(), record, registry)
	require.NoError(t, err)
	gate, ok := value.Records.Get(ctx, "gate")
	require.True(t, ok)
	require.Equal(t, "open", string(gate.Data))
	detached := ctx.(*immutableCtx).root
	require.NotNil(t, detached.bounded)
	child := detached.children["Child"]
	attrs := child.serializedNode.Metadata.GetComponentAttributes()
	for _, task := range append(attrs.PureTasks, attrs.SideEffectTasks...) {
		_, err := child.deserializeComponentTask(task)
		require.NoError(t, err)
	}
	require.Len(t, detached.Snapshot(nil).Nodes, len(all))
}
func TestBoundedWorkPendingMetadataMatchesDefault(t *testing.T) {
	t.Parallel()
	raw, left := newWorkTaskTree(t, false, false)
	bounded, right := newWorkTaskTree(t, true, false)
	for i, root := range []*Node{raw, bounded} {
		ctx := NewMutableContext(t.Context(), root)
		_, err := root.Component(ctx, ComponentRef{})
		require.NoError(t, err)
		child := []*workTaskRoot{left, right}[i].Child.Get(ctx)
		require.NoError(t, ctx.SetRequestLinks(child, "request", []*commonpb.Link{{}}))
		closeDataMap(t, root)
	}
	compareWorkTaskTrees(t, raw, bounded)
}

func TestBoundedTaskChildReceiverScope(t *testing.T) {
	t.Parallel()
	for _, bounded := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "bounded"}[bounded], func(t *testing.T) {
			t.Parallel()
			root, value := newWorkTaskTree(t, bounded, false)
			ctx := NewMutableContext(t.Context(), root)
			_, err := root.Component(ctx, ComponentRef{})
			require.NoError(t, err)
			sibling := &workTaskChild{State: &commonpb.Payload{}}
			value.Sibling = NewComponentField(ctx, sibling)
			ctx.AddTask(sibling, TaskAttributes{ScheduledTime: time.Unix(200, 0)}, &commonpb.Payload{})
			ctx.AddTask(sibling, TaskAttributes{ScheduledTime: time.Unix(300, 0)}, &commonpb.Header{})
			closeDataMap(t, root)
			child := root.children["Child"]
			before := proto.Clone(root.children["Sibling"].serializedNode)
			var paths [][]string
			for path := range child.componentNodes() {
				paths = append(paths, path)
			}
			require.Len(t, paths, 1)
			require.Empty(t, paths[0])
			require.NoError(t, child.RefreshTasks())
			require.True(t, proto.Equal(before, root.children["Sibling"].serializedNode))
			executions := 0
			require.NoError(t, child.EachPureTask(time.Unix(200, 0), func(handler NodePureTask, attrs TaskAttributes, payload any) (bool, error) {
				ran, err := handler.ExecutePureTask(t.Context(), attrs, payload)
				if ran {
					executions++
				}
				return ran, err
			}))
			require.Equal(t, 1, executions)
			require.Empty(t, sibling.State.Data)
		})
	}
}

func TestBoundedChildDrainPreservesSiblingWork(t *testing.T) {
	t.Parallel()
	root, value := newWorkTaskTree(t, true, false)
	ctx := NewMutableContext(t.Context(), root)
	_, err := root.Component(ctx, ComponentRef{})
	require.NoError(t, err)
	value.Sibling = NewComponentField(ctx, &workTaskChild{State: &commonpb.Payload{}})
	closeDataMap(t, root)
	ctx = NewMutableContext(t.Context(), root)
	_, err = root.Component(ctx, ComponentRef{})
	require.NoError(t, err)
	value.Child.Get(ctx).State.Data = []byte("child")
	value.Sibling.Get(ctx).State.Data = []byte("sibling")
	child, sibling := root.children["Child"], root.children["Sibling"]
	for range 2 {
		require.NoError(t, child.syncSubComponents())
		require.Equal(t, valueStateNeedSyncStructure, sibling.valueState)
		require.Equal(t, valueStateNeedSyncStructure, root.valueState)
	}
	require.NoError(t, child.closeTransactionSerializeNodes())
	require.Equal(t, valueStateNeedSyncStructure, sibling.valueState)
	mutation := closeDataMap(t, root)
	require.Contains(t, mutation.UpdatedNodes, "Sibling")
	fresh := reloadMemoTree(t, root, memoSnapshot(root))
	c, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
	require.NoError(t, err)
	read := NewContext(t.Context(), fresh)
	require.Equal(t, "sibling", string(c.(*workTaskRoot).Sibling.Get(read).State.Data))
	require.Equal(t, "child", string(c.(*workTaskRoot).Child.Get(read).State.Data))
}
