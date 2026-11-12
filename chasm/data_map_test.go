//go:build test_dep

package chasm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

type dataMapFixture struct {
	UnimplementedComponent
	State   *persistencespb.WorkflowExecutionState
	Records DataMap[*commonpb.Payload]
	Other   DataMap[*commonpb.Payload]
}

func (*dataMapFixture) LifecycleState(Context) LifecycleState     { return LifecycleStateRunning }
func (*dataMapFixture) ContextMetadata(Context) map[string]string { return nil }
func (*dataMapFixture) Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error) {
	return TerminateComponentResponse{}, nil
}

type dataMapLibrary struct{ UnimplementedLibrary }

func (*dataMapLibrary) Name() string { return "datamapfixture" }
func (*dataMapLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{NewRegistrableComponent[*dataMapFixture]("root")}
}

func newDataMapFixture(t *testing.T) (*Node, *dataMapFixture, MutableContext) {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	registry := NewRegistry(logger)
	require.NoError(t, registry.Register(&CoreLibrary{}))
	require.NoError(t, registry.Register(&dataMapLibrary{}))
	backend := &workFixtureBackend{MockNodeBackend: &MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }}, counts: &WorkCountersForTest{}}
	root := NewEmptyTree(registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := NewMutableContext(t.Context(), root)
	value := &dataMapFixture{State: &persistencespb.WorkflowExecutionState{}, Records: NewDataMap[*commonpb.Payload](ctx)}
	return root, value, ctx
}
func reloadDataMapFixture(t *testing.T, root *Node) (*Node, *dataMapFixture) {
	t.Helper()
	saved := root.Snapshot(nil).Nodes
	copied := make(map[string]*persistencespb.ChasmNode, len(saved))
	for k, v := range saved {
		copied[k] = proto.Clone(v).(*persistencespb.ChasmNode)
	}
	fresh, err := NewTreeFromDB(copied, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	c, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
	require.NoError(t, err)
	return fresh, c.(*dataMapFixture)
}
func closeDataMap(t *testing.T, root *Node) NodesMutation {
	t.Helper()
	mutation, err := root.CloseTransaction(t.Context(), true)
	require.NoError(t, err)
	for key := range mutation.UpdatedNodes {
		_, deleted := mutation.DeletedNodes[key]
		require.False(t, deleted)
	}
	return mutation
}

func TestDataMapDetachedOwnership(t *testing.T) {
	t.Parallel()
	root, value, ctx := newDataMapFixture(t)
	input := &commonpb.Payload{Data: []byte("original")}
	input.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	value.Records.Put(ctx, "key", input)
	input.Data[0] = 'x'
	got, ok := value.Records.Get(ctx, "key")
	require.True(t, ok)
	require.Equal(t, "original", string(got.Data))
	got.Data[0] = 'y'
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	require.Panics(t, func() { value.Records.Get(ctx, "key") })
	current := NewMutableContext(t.Context(), root)
	got, ok = value.Records.Get(ContextWithValue(current, "unused", true), "key")
	require.True(t, ok)
	require.Equal(t, "original", string(got.Data))
	require.Equal(t, []byte{0xa0, 0x06, 0x01}, []byte(got.ProtoReflect().GetUnknown()))
	require.False(t, root.IsDirty())
	fresh, loaded := reloadDataMapFixture(t, root)
	require.Panics(t, func() { value.Records.Put(NewMutableContext(t.Context(), fresh), "key", input) })
	got, ok = loaded.Records.Get(NewContext(t.Context(), fresh), "key")
	require.True(t, ok)
	require.Equal(t, "original", string(got.Data))
	require.Panics(t, func() { loaded.Records.Put(NewMutableContext(t.Context(), fresh), "nil", nil) })
}

func TestDataMapCoalescedDeltas(t *testing.T) {
	t.Parallel()
	for _, ops := range []string{"p", "pd", "dp", "pdp", "d", "dd", "dpd"} {
		t.Run(ops, func(t *testing.T) {
			t.Parallel()
			for _, existing := range []bool{false, true} {
				root, value, ctx := newDataMapFixture(t)
				if existing {
					value.Records.Put(ctx, "key", &commonpb.Payload{Data: []byte("old")})
				}
				require.NoError(t, root.SetRootComponent(value))
				closeDataMap(t, root)
				ctx = NewMutableContext(t.Context(), root)
				for _, op := range ops {
					if op == 'p' {
						value.Records.Put(ctx, "key", &commonpb.Payload{Data: []byte("new")})
					} else {
						value.Records.Delete(ctx, "key")
					}
				}
				closeDataMap(t, root)
				fresh, loaded := reloadDataMapFixture(t, root)
				got, ok := loaded.Records.Get(NewContext(t.Context(), fresh), "key")
				require.Equal(t, ops[len(ops)-1] == 'p', ok)
				if ok {
					require.Equal(t, "new", string(got.Data))
				}
			}
		})
	}
}

func TestDataMapLazyHydrationAndCorruption(t *testing.T) {
	t.Parallel()
	for _, size := range []int{0, 4, 5, 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			t.Parallel()
			root, value, ctx := newDataMapFixture(t)
			for i := range size {
				value.Records.Put(ctx, fmt.Sprint(i), &commonpb.Payload{Data: []byte("unrelated")})
			}
			value.Records.Put(ctx, "good", &commonpb.Payload{Data: []byte("good")})
			value.Records.Put(ctx, "bad", &commonpb.Payload{})
			require.NoError(t, root.SetRootComponent(value))
			closeDataMap(t, root)
			root.children["Records"].children["bad"].serializedNode.Data = &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte{0xff}}
			backend := root.backend.(*workFixtureBackend)
			backend.counts.Reset()
			fresh, loaded := reloadDataMapFixture(t, root)
			require.Zero(t, backend.counts.Snapshot()["map_hydration_keys"])
			require.Zero(t, backend.counts.Snapshot()["data_decode_calls"])
			require.EqualValues(t, size+4, backend.counts.Snapshot()["cold_nodes"])
			counts := fresh.BeginWorkInspectionForTest()
			current := NewMutableContext(t.Context(), fresh)
			got, ok := loaded.Records.Get(current, "good")
			require.True(t, ok)
			require.Equal(t, "good", string(got.Data))
			require.False(t, fresh.IsDirty())
			require.EqualValues(t, 1, counts.Snapshot()["data_decode_calls"])
			require.Zero(t, counts.Snapshot()["map_hydration_keys"])
			loaded.Records.Put(current, "replacement", &commonpb.Payload{})
			loaded.Records.Delete(current, "missing")
			closeDataMap(t, fresh)
			require.Panics(t, func() { loaded.Records.Get(NewContext(t.Context(), fresh), "bad") })
		})
	}
}

func TestDataMapAttachmentAndReplicationIdentity(t *testing.T) {
	t.Parallel()
	t.Run("double attachment", func(t *testing.T) {
		t.Parallel()
		root, value, _ := newDataMapFixture(t)
		value.Other = value.Records
		require.ErrorContains(t, root.SetRootComponent(value), "multiple")
	})
	t.Run("replication invalidates old owner", func(t *testing.T) {
		t.Parallel()
		root, value, ctx := newDataMapFixture(t)
		value.Records.Put(ctx, "key", &commonpb.Payload{})
		require.NoError(t, root.SetRootComponent(value))
		closeDataMap(t, root)
		child := root.children["Records"].children["key"]
		path, err := child.getEncodedPath()
		require.NoError(t, err)
		incoming := proto.Clone(child.serializedNode).(*persistencespb.ChasmNode)
		incoming.Data, err = encodeChasmBlob(&commonpb.Payload{Data: []byte("replicated")})
		require.NoError(t, err)
		incoming.Metadata.LastUpdateVersionedTransition.TransitionCount++
		require.NoError(t, root.ApplyMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{path: incoming}}))
		require.Panics(t, func() { value.Records.Get(NewContext(t.Context(), root), "key") })
		c, err := root.Component(NewContext(t.Context(), root), ComponentRef{})
		require.NoError(t, err)
		got, ok := c.(*dataMapFixture).Records.Get(NewContext(t.Context(), root), "key")
		require.True(t, ok)
		require.Equal(t, "replicated", string(got.Data))
	})
	t.Run("removed owner", func(t *testing.T) {
		t.Parallel()
		root, value, _ := newDataMapFixture(t)
		require.NoError(t, root.SetRootComponent(value))
		closeDataMap(t, root)
		replacement := &dataMapFixture{State: &persistencespb.WorkflowExecutionState{}}
		require.NoError(t, root.SetRootComponent(replacement))
		require.Panics(t, func() { value.Records.Get(NewContext(t.Context(), root), "key") })
	})
}

func TestDataMapNoOpDoesNotPublish(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"missing-delete", "new-put-delete", "read"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			root, value, ctx := newDataMapFixture(t)
			value.Records.Put(ctx, "present", &commonpb.Payload{})
			require.NoError(t, root.SetRootComponent(value))
			closeDataMap(t, root)
			before := proto.Clone(root.serializedNode)
			ctx = NewMutableContext(t.Context(), root)
			switch op {
			case "missing-delete":
				value.Records.Delete(ctx, "missing")
				require.False(t, root.IsDirty())
			case "new-put-delete":
				value.Records.Put(ctx, "missing", &commonpb.Payload{})
				value.Records.Delete(ctx, "missing")
			case "read":
				_, ok := value.Records.Get(ctx, "present")
				require.True(t, ok)
				require.False(t, root.IsDirty())
			default:
				t.Fatalf("unexpected operation %q", op)
			}
			mutation := closeDataMap(t, root)
			require.Empty(t, mutation.UpdatedNodes)
			require.Empty(t, mutation.DeletedNodes)
			require.True(t, proto.Equal(before, root.serializedNode))
		})
	}
}

func TestDataMapDetachedAndFailedPublication(t *testing.T) {
	t.Parallel()
	root, value, ctx := newDataMapFixture(t)
	value.Records.Put(ctx, "key", &commonpb.Payload{Data: []byte("committed")})
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	durable := &persistencespb.WorkflowMutableState{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionState: &persistencespb.WorkflowExecutionState{}, ChasmNodes: make(map[string]*persistencespb.ChasmNode)}
	for key, node := range root.Snapshot(nil).Nodes {
		durable.ChasmNodes[key] = proto.Clone(node).(*persistencespb.ChasmNode)
	}
	ctx = NewMutableContext(t.Context(), root)
	value.Records.Put(ctx, "key", &commonpb.Payload{Data: []byte("rejected publication")})
	// Close succeeds but the returned mutation is deliberately not applied to the
	// durable record, modeling a rejected publication followed by lease discard.
	closeDataMap(t, root)
	detached, readCtx, err := NewDetachedExecution[*dataMapFixture](t.Context(), durable, root.registry)
	require.NoError(t, err)
	got, ok := detached.Records.Get(readCtx, "key")
	require.True(t, ok)
	require.Equal(t, "committed", string(got.Data))
	got.Data[0] = 'X'
	again, ok := detached.Records.Get(readCtx, "key")
	require.True(t, ok)
	require.Equal(t, "committed", string(again.Data))
	_, mutable := readCtx.(MutableContext)
	require.False(t, mutable)
	require.Panics(t, func() { value.Records.Get(readCtx, "key") })
	require.Panics(t, func() { value.Records.Put(ctx, "key", &commonpb.Payload{}) })
}

func TestDataMapInterleavedSynchronization(t *testing.T) {
	t.Parallel()
	for _, ops := range []string{"dp", "dpd", "dpdp", "pd", "pdp"} {
		t.Run(ops, func(t *testing.T) {
			t.Parallel()
			for _, existing := range []bool{false, true} {
				root, value, ctx := newDataMapFixture(t)
				if existing {
					value.Records.Put(ctx, "key", &commonpb.Payload{Data: []byte("old")})
				}
				require.NoError(t, root.SetRootComponent(value))
				closeDataMap(t, root)
				committed := root.Snapshot(nil).Nodes
				durable := make(map[string]*persistencespb.ChasmNode, len(committed))
				for k, v := range committed {
					durable[k] = proto.Clone(v).(*persistencespb.ChasmNode)
				}
				ctx = NewMutableContext(t.Context(), root)
				for _, op := range ops {
					if op == 'p' {
						value.Records.Put(ctx, "key", &commonpb.Payload{Data: []byte("new")})
					} else {
						value.Records.Delete(ctx, "key")
					}
					require.NoError(t, root.syncSubComponents())
				}
				mutation := closeDataMap(t, root)
				for k := range mutation.DeletedNodes {
					delete(durable, k)
				}
				for k, v := range mutation.UpdatedNodes {
					durable[k] = proto.Clone(v).(*persistencespb.ChasmNode)
				}
				fresh, err := NewTreeFromDB(durable, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
				require.NoError(t, err)
				c, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
				require.NoError(t, err)
				got, ok := c.(*dataMapFixture).Records.Get(NewContext(t.Context(), fresh), "key")
				require.Equal(t, ops[len(ops)-1] == 'p', ok)
				if ok {
					require.Equal(t, "new", string(got.Data))
				}
			}
		})
	}
}

func TestDataMapRejectsCorruptKindAndReplacedOwner(t *testing.T) {
	t.Parallel()
	t.Run("wrong kind is not absence", func(t *testing.T) {
		t.Parallel()
		root, value, ctx := newDataMapFixture(t)
		value.Records.Put(ctx, "key", &commonpb.Payload{})
		require.NoError(t, root.SetRootComponent(value))
		closeDataMap(t, root)
		root.children["Records"].children["key"].initSerializedCollectionNode()
		require.Panics(t, func() { value.Records.Get(NewContext(t.Context(), root), "key") })
	})
	t.Run("pending delta does not move to replacement", func(t *testing.T) {
		t.Parallel()
		root, value, _ := newDataMapFixture(t)
		require.NoError(t, root.SetRootComponent(value))
		closeDataMap(t, root)
		committed, _ := reloadDataMapFixture(t, root)
		ctx := NewMutableContext(t.Context(), root)
		value.Records.Put(ctx, "key", &commonpb.Payload{})
		require.ErrorContains(t, root.SetRootComponent(&dataMapFixture{State: &persistencespb.WorkflowExecutionState{}}), "removed or replaced")
		mutation, err := root.CloseTransaction(t.Context(), true)
		require.ErrorContains(t, err, "removed or replaced")
		require.Empty(t, mutation.UpdatedNodes)
		require.Panics(t, func() { value.Records.Get(ctx, "key") })
		c, err := committed.Component(NewContext(t.Context(), committed), ComponentRef{})
		require.NoError(t, err)
		_, ok := c.(*dataMapFixture).Records.Get(NewContext(t.Context(), committed), "key")
		require.False(t, ok)
	})
}

func TestDataMapSerializerFailureReload(t *testing.T) {
	t.Parallel()
	root, value, ctx := newDataMapFixture(t)
	value.Records.Put(ctx, "key", &commonpb.Payload{Data: []byte("committed")})
	require.NoError(t, root.SetRootComponent(value))
	closeDataMap(t, root)
	committed, loaded := reloadDataMapFixture(t, root)
	ctx = NewMutableContext(t.Context(), root)
	value.Records.Put(ctx, "key", &commonpb.Payload{Metadata: map[string][]byte{string([]byte{0xff}): {1}}})
	mutation, err := root.CloseTransaction(t.Context(), true)
	require.Error(t, err)
	require.Empty(t, mutation.UpdatedNodes)
	require.Panics(t, func() { value.Records.Get(ctx, "key") })
	got, ok := loaded.Records.Get(NewContext(t.Context(), committed), "key")
	require.True(t, ok)
	require.Equal(t, "committed", string(got.Data))
}
