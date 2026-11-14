//go:build test_dep

package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

type boundedFixtureLibrary struct {
	UnimplementedLibrary
	layout BoundedDataLayout
	raw    bool
}

func (*boundedFixtureLibrary) Name() string { return "boundedfixture" }
func (l *boundedFixtureLibrary) Components() []*RegistrableComponent {
	if l.raw {
		return []*RegistrableComponent{NewRegistrableComponent[*workFixture]("root", WithBoundedDataLayout(l.layout))}
	}
	return []*RegistrableComponent{NewRegistrableComponent[*dataMapFixture]("root", WithBoundedDataLayout(l.layout))}
}
func dataOnlyLayout() BoundedDataLayout {
	return BoundedDataLayout{Components: []BoundedComponentSlot{BoundedComponent[*dataMapFixture](nil, BoundedTaskLimits{})}}
}
func boundedFixtureTree(t *testing.T) (*Node, *dataMapFixture, MutableContext) {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	registry := NewRegistry(logger)
	require.NoError(t, registry.Register(&CoreLibrary{}))
	require.NoError(t, registry.Register(&boundedFixtureLibrary{layout: dataOnlyLayout()}))
	backend := &MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }}
	root := NewEmptyTree(registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := NewMutableContext(t.Context(), root)
	value := &dataMapFixture{State: &persistencespb.WorkflowExecutionState{}, Records: NewDataMap[*commonpb.Payload](ctx)}
	return root, value, ctx
}
func TestBoundedLayoutColdTopology(t *testing.T) {
	t.Parallel()
	root, value, ctx := boundedFixtureTree(t)
	value.Records.Put(ctx, "one", &commonpb.Payload{Data: []byte("value")})
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	require.Len(t, root.bounded.slots, 1)
	require.Same(t, root, root.bounded.slots[0])
	saved := memoSnapshot(root)
	fresh := reloadMemoTree(t, root, saved)
	require.Len(t, fresh.bounded.slots, 1)
	require.Same(t, fresh, fresh.bounded.slots[0])
	c, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
	require.NoError(t, err)
	got, ok := c.(*dataMapFixture).Records.Get(NewContext(t.Context(), fresh), "one")
	require.True(t, ok)
	require.Equal(t, "value", string(got.Data))
	require.Len(t, fresh.Snapshot(nil).Nodes, len(saved))
	for _, corruption := range []string{"undeclared component", "nested DATA", "task limit", "pointer"} {
		t.Run(corruption, func(t *testing.T) {
			t.Parallel()
			input := make(map[string]*persistencespb.ChasmNode, len(saved))
			for k, v := range saved {
				input[k] = proto.Clone(v).(*persistencespb.ChasmNode)
			}
			switch corruption {
			case "undeclared component":
				input["Records#one"] = proto.Clone(saved[""]).(*persistencespb.ChasmNode)
			case "nested DATA":
				input["Records#one$nested"] = proto.Clone(saved["Records#one"]).(*persistencespb.ChasmNode)
			case "task limit":
				input[""].Metadata.GetComponentAttributes().PureTasks = []*persistencespb.ChasmComponentAttributes_Task{{}}
			case "pointer":
				input["Records#one"].Metadata.Attributes = &persistencespb.ChasmNodeMetadata_PointerAttributes{PointerAttributes: &persistencespb.ChasmPointerAttributes{}}
			default:
				t.Fatal("unexpected corruption")
			}
			_, err := NewTreeFromDB(input, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
			require.Error(t, err)
		})
	}
}
func TestBoundedLayoutRegistration(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"empty", "missing root", "duplicate", "negative", "raw map", "undeclared slot"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			layout := dataOnlyLayout()
			raw := false
			switch name {
			case "empty":
				layout.Components = nil
			case "missing root":
				layout.Components = []BoundedComponentSlot{BoundedComponent[*dataMapFixture]([]string{"Child"}, BoundedTaskLimits{})}
			case "duplicate":
				layout.Components = append(layout.Components, layout.Components[0])
			case "negative":
				layout.MaxImmediateExecutions = -1
			case "raw map":
				raw = true
				layout.Components = []BoundedComponentSlot{BoundedComponent[*workFixture](nil, BoundedTaskLimits{})}
			case "undeclared slot":
				layout.Components = append(layout.Components, BoundedComponent[*dataMapFixture]([]string{"Child"}, BoundedTaskLimits{}))
			default:
				t.Fatal("unexpected case")
			}
			registry := NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
			require.Error(t, registry.Register(&boundedFixtureLibrary{layout: layout, raw: raw}))
		})
	}
}
func TestBoundedLayoutReplicationRejectsHiddenComponent(t *testing.T) {
	t.Parallel()
	root, value, ctx := boundedFixtureTree(t)
	value.Records.Put(ctx, "one", &commonpb.Payload{})
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	incoming := proto.Clone(root.serializedNode).(*persistencespb.ChasmNode)
	incoming.Metadata.LastUpdateVersionedTransition.TransitionCount++
	err := root.ApplyMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{"Records#hidden": incoming}})
	require.ErrorContains(t, err, "unexpected bounded layout node")
}

type boundedParent struct {
	UnimplementedComponent
	State *commonpb.Payload
	Child Field[*dataMapFixture]
}

func (*boundedParent) LifecycleState(Context) LifecycleState     { return LifecycleStateRunning }
func (*boundedParent) ContextMetadata(Context) map[string]string { return nil }
func (*boundedParent) Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error) {
	return TerminateComponentResponse{}, nil
}

type boundedParentLibrary struct{ UnimplementedLibrary }

func (*boundedParentLibrary) Name() string { return "boundedparent" }
func (*boundedParentLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{
		NewRegistrableComponent[*boundedParent]("root", WithBoundedDataLayout(BoundedDataLayout{Components: []BoundedComponentSlot{BoundedComponent[*boundedParent](nil, BoundedTaskLimits{}), BoundedComponent[*dataMapFixture]([]string{"Child"}, BoundedTaskLimits{})}})),
		NewRegistrableComponent[*dataMapFixture]("child"),
	}
}
func TestBoundedLayoutNestedSlotLifetime(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	registry := NewRegistry(logger)
	require.NoError(t, registry.Register(&CoreLibrary{}))
	require.NoError(t, registry.Register(&boundedParentLibrary{}))
	backend := &MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }}
	root := NewEmptyTree(registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	value := &boundedParent{State: &commonpb.Payload{}}
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	require.Nil(t, root.bounded.slots[1])
	ctx := NewMutableContext(t.Context(), root)
	_, err := root.Component(ctx, ComponentRef{})
	require.NoError(t, err)
	child := &dataMapFixture{State: &persistencespb.WorkflowExecutionState{}, Records: NewDataMap[*commonpb.Payload](ctx)}
	value.Child = NewComponentField(ctx, child)
	closeDataMap(t, root)
	require.Same(t, root.children["Child"], root.bounded.slots[1])
	before := proto.Clone(root.children["Child"].serializedNode).(*persistencespb.ChasmNode)
	ctx = NewMutableContext(t.Context(), root)
	_, err = root.Component(ctx, ComponentRef{})
	require.NoError(t, err)
	value.Child = NewEmptyField[*dataMapFixture]()
	closeDataMap(t, root)
	require.Nil(t, root.bounded.slots[1])
	before.Metadata.LastUpdateVersionedTransition.TransitionCount++
	require.NoError(t, root.ApplySystemMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{"Child": before}}))
	require.Same(t, root.children["Child"], root.bounded.slots[1])
	replacement := proto.Clone(before).(*persistencespb.ChasmNode)
	replacement.Metadata.LastUpdateVersionedTransition.TransitionCount++
	require.NoError(t, root.ApplyMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{"Child": replacement}}))
	require.Same(t, root.children["Child"], root.bounded.slots[1])
	require.NoError(t, root.ApplyMutation(NodesMutation{DeletedNodes: map[string]struct{}{"Child": {}}}))
	require.Nil(t, root.bounded.slots[1])
}
func TestBoundedLayoutActiveBulkRemovalRejected(t *testing.T) {
	t.Parallel()
	root, value, ctx := boundedFixtureTree(t)
	value.Records.Put(ctx, "key", &commonpb.Payload{})
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	oldSlot := root.bounded.slots[0]
	require.ErrorContains(t, root.SetRootComponent(&dataMapFixture{State: &persistencespb.WorkflowExecutionState{}}), "incremental DATA cleanup")
	require.Same(t, oldSlot, root.bounded.slots[0])
	require.Same(t, value, root.value)
}
func TestBoundedLayoutSystemMutationRejectsHiddenComponent(t *testing.T) {
	t.Parallel()
	root, value, ctx := boundedFixtureTree(t)
	value.Records.Put(ctx, "one", &commonpb.Payload{})
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	incoming := proto.Clone(root.serializedNode).(*persistencespb.ChasmNode)
	incoming.Metadata.LastUpdateVersionedTransition.TransitionCount++
	require.ErrorContains(t, root.ApplySystemMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{"Records#hidden": incoming}}), "unexpected bounded layout node")
}
