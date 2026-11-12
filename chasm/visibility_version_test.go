//go:build test_dep

package chasm

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

type memoCallKey struct{}
type memoCalls struct {
	count int
	allow bool
}
type memoVersionFixture struct {
	UnimplementedComponent
	State      *commonpb.Payload
	Content    Field[*commonpb.Payload]
	Revision   Field[*commonpb.Payload]
	Visibility Field[*Visibility]
}

func (*memoVersionFixture) LifecycleState(Context) LifecycleState     { return LifecycleStateRunning }
func (*memoVersionFixture) ContextMetadata(Context) map[string]string { return nil }
func (*memoVersionFixture) Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error) {
	return TerminateComponentResponse{}, nil
}
func (v *memoVersionFixture) MemoVersion(ctx Context) uint64 {
	if revision, ok := v.Revision.TryGet(ctx); ok {
		return binary.BigEndian.Uint64(revision.Data)
	}
	return binary.BigEndian.Uint64(v.State.Data)
}
func (v *memoVersionFixture) Memo(ctx Context) proto.Message {
	calls := ctx.Value(memoCallKey{}).(*memoCalls)
	calls.count++
	if !calls.allow {
		panic("full Memo must not run on versioned mutation")
	}
	return v.Content.Get(ctx)
}

type rawMemoFixture memoVersionFixture

func (*rawMemoFixture) LifecycleState(Context) LifecycleState     { return LifecycleStateRunning }
func (*rawMemoFixture) ContextMetadata(Context) map[string]string { return nil }
func (*rawMemoFixture) Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error) {
	return TerminateComponentResponse{}, nil
}
func (v *rawMemoFixture) Memo(ctx Context) proto.Message { return (*memoVersionFixture)(v).Memo(ctx) }

type memoVersionLibrary struct {
	UnimplementedLibrary
	calls *memoCalls
	raw   bool
}

func (*memoVersionLibrary) Name() string { return "memoversion" }
func (l *memoVersionLibrary) Components() []*RegistrableComponent {
	options := []RegistrableComponentOption{WithBusinessIDAlias("MemoID"), WithContextValues(map[any]any{memoCallKey{}: l.calls})}
	if l.raw {
		return []*RegistrableComponent{NewRegistrableComponent[*rawMemoFixture]("root", options...)}
	}
	return []*RegistrableComponent{NewRegistrableComponent[*memoVersionFixture]("root", options...)}
}
func newMemoVersionTree(t *testing.T, raw bool) (*Node, *memoVersionFixture, *memoCalls) {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	calls := &memoCalls{allow: raw}
	registry := NewRegistry(logger)
	require.NoError(t, registry.Register(&CoreLibrary{}))
	require.NoError(t, registry.Register(&memoVersionLibrary{calls: calls, raw: raw}))
	backend := &MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }}
	root := NewEmptyTree(registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := NewMutableContext(t.Context(), root)
	value := &memoVersionFixture{State: &commonpb.Payload{Data: make([]byte, 8)}, Content: NewDataField(ctx, &commonpb.Payload{Data: make([]byte, 1<<20)}), Visibility: NewComponentField(ctx, NewVisibility(ctx))}
	binary.BigEndian.PutUint64(value.State.Data, 1)
	if raw {
		require.NoError(t, root.SetRootComponent((*rawMemoFixture)(value)))
	} else {
		require.NoError(t, root.SetRootComponent(value))
	}
	closeDataMap(t, root)
	return root, value, calls
}
func memoSnapshot(root *Node) map[string]*persistencespb.ChasmNode {
	saved := make(map[string]*persistencespb.ChasmNode)
	for key, value := range root.Snapshot(nil).Nodes {
		saved[key] = proto.Clone(value).(*persistencespb.ChasmNode)
	}
	return saved
}
func reloadMemoTree(t *testing.T, root *Node, saved map[string]*persistencespb.ChasmNode) *Node {
	t.Helper()
	copied := make(map[string]*persistencespb.ChasmNode, len(saved))
	for key, value := range saved {
		copied[key] = proto.Clone(value).(*persistencespb.ChasmNode)
	}
	fresh, err := NewTreeFromDB(copied, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	return fresh
}
func TestVisibilityMemoVersionSkipsAssembly(t *testing.T) {
	t.Parallel()
	root, value, calls := newMemoVersionTree(t, false)
	require.Zero(t, calls.count)
	require.Nil(t, root.currentMemo)
	require.EqualValues(t, 1, root.currentMemoVersion)
	for step := range 2 {
		ctx := NewMutableContext(t.Context(), root)
		_, err := root.Component(ctx, ComponentRef{})
		require.NoError(t, err)
		value.State.Metadata = map[string][]byte{"unrelated": {byte(step)}}
		mutation := closeDataMap(t, root)
		_, changed := mutation.UpdatedNodes["Visibility"]
		require.False(t, changed)
	}
	ctx := NewMutableContext(t.Context(), root)
	_, err := root.Component(ctx, ComponentRef{})
	require.NoError(t, err)
	value.Content.Get(ctx).Data[0] = 1
	binary.BigEndian.PutUint64(value.State.Data, 2)
	mutation := closeDataMap(t, root)
	require.Contains(t, mutation.UpdatedNodes, "Visibility")
	require.Zero(t, calls.count)
	fresh := reloadMemoTree(t, root, memoSnapshot(root))
	require.Zero(t, calls.count)
	require.EqualValues(t, 2, fresh.currentMemoVersion)
	pendingBefore := proto.Clone(fresh.children["Visibility"].serializedNode)
	mutation = closeDataMap(t, fresh)
	require.NotContains(t, mutation.UpdatedNodes, "Visibility")
	require.True(t, proto.Equal(pendingBefore, fresh.children["Visibility"].serializedNode))
	calls.allow = true
	got := value.Memo(NewContext(t.Context(), root)).(*commonpb.Payload)
	require.Len(t, got.Data, 1<<20)
	require.Equal(t, 1, calls.count)
}
func TestVisibilityMemoVersionFailedPublicationAndLimits(t *testing.T) {
	t.Parallel()
	root, value, calls := newMemoVersionTree(t, false)
	saved := memoSnapshot(root)
	ctx := NewMutableContext(t.Context(), root)
	_, err := root.Component(ctx, ComponentRef{})
	require.NoError(t, err)
	binary.BigEndian.PutUint64(value.State.Data, math.MaxUint64)
	mutation := closeDataMap(t, root)
	require.Contains(t, mutation.UpdatedNodes, "Visibility")
	// Reject this publication: only the original committed nodes are reloaded.
	fresh := reloadMemoTree(t, root, saved)
	require.EqualValues(t, 1, fresh.currentMemoVersion)
	c, err := fresh.Component(NewMutableContext(t.Context(), fresh), ComponentRef{})
	require.NoError(t, err)
	retry := c.(*memoVersionFixture)
	binary.BigEndian.PutUint64(retry.State.Data, math.MaxUint64)
	mutation = closeDataMap(t, fresh)
	require.Contains(t, mutation.UpdatedNodes, "Visibility")
	require.Zero(t, calls.count)
	_, err = fresh.Component(NewMutableContext(t.Context(), fresh), ComponentRef{})
	require.NoError(t, err)
	binary.BigEndian.PutUint64(retry.State.Data, 1)
	mutation, err = fresh.CloseTransaction(t.Context(), true)
	require.ErrorContains(t, err, "decrease or wrap")
	require.Empty(t, mutation.UpdatedNodes)
	fresh = reloadMemoTree(t, root, saved)
	c, err = fresh.Component(NewMutableContext(t.Context(), fresh), ComponentRef{})
	require.NoError(t, err)
	binary.BigEndian.PutUint64(c.(*memoVersionFixture).State.Data, 0)
	_, err = fresh.CloseTransaction(t.Context(), true)
	require.ErrorContains(t, err, "nonzero")
}
func TestVisibilityMemoVersionPassiveAndDetached(t *testing.T) {
	t.Parallel()
	root, initial, calls := newMemoVersionTree(t, false)
	mutable := NewMutableContext(t.Context(), root)
	_, err := root.Component(mutable, ComponentRef{})
	require.NoError(t, err)
	initial.Revision = NewDataField(mutable, &commonpb.Payload{Data: append([]byte(nil), initial.State.Data...)})
	closeDataMap(t, root)
	updated := proto.Clone(root.children["Revision"].serializedNode).(*persistencespb.ChasmNode)
	version := &commonpb.Payload{Data: make([]byte, 8)}
	binary.BigEndian.PutUint64(version.Data, 2)
	updated.Data, err = encodeChasmBlob(version)
	require.NoError(t, err)
	updated.Metadata.LastUpdateVersionedTransition.TransitionCount++
	child := proto.Clone(root.children["Content"].serializedNode).(*persistencespb.ChasmNode)
	child.Data, err = encodeChasmBlob(&commonpb.Payload{Data: []byte("replicated output")})
	require.NoError(t, err)
	child.Metadata.LastUpdateVersionedTransition.TransitionCount++
	require.NoError(t, root.ApplyMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{"Revision": updated, "Content": child}}))
	child = proto.Clone(root.children["Content"].serializedNode).(*persistencespb.ChasmNode)
	child.Metadata.LastUpdateVersionedTransition.TransitionCount++
	require.NoError(t, root.ApplyMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{"Content": child}}))
	require.EqualValues(t, 2, root.currentMemoVersion)
	require.Zero(t, calls.count)
	// Snapshot replication refreshes the same version cache from persisted DATA.
	snapshot := memoSnapshot(root)
	binary.BigEndian.PutUint64(version.Data, 3)
	snapshot["Revision"].Data, err = encodeChasmBlob(version)
	require.NoError(t, err)
	snapshot["Revision"].Metadata.LastUpdateVersionedTransition.TransitionCount++
	require.NoError(t, root.ApplySnapshot(NodesSnapshot{Nodes: snapshot}))
	require.EqualValues(t, 3, root.currentMemoVersion)
	require.Zero(t, calls.count)
	// Passive cache refresh never creates a visibility update to compensate for
	// cache invalidation; the replicated task is the export authority.
	before := proto.Clone(root.children["Visibility"].serializedNode)
	mutation, err := root.CloseTransaction(t.Context(), false)
	require.NoError(t, err)
	require.NotContains(t, mutation.UpdatedNodes, "Visibility")
	require.True(t, proto.Equal(before, root.children["Visibility"].serializedNode))
	saved := &persistencespb.WorkflowMutableState{ChasmNodes: memoSnapshot(root), ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionState: &persistencespb.WorkflowExecutionState{}}
	value, ctx, err := NewDetachedExecution[*memoVersionFixture](t.Context(), saved, root.registry)
	require.NoError(t, err)
	require.EqualValues(t, 3, value.MemoVersion(ctx))
	require.Zero(t, calls.count)
	calls.allow = true
	require.Equal(t, "replicated output", string(value.Memo(ctx).(*commonpb.Payload).Data))
}
func TestVisibilityMemoVersionDefaultProviderParity(t *testing.T) {
	t.Parallel()
	root, value, calls := newMemoVersionTree(t, true)
	require.Equal(t, 1, calls.count)
	require.NotNil(t, root.currentMemo)
	require.False(t, root.hasCurrentMemoVersion)
	_, err := root.Component(NewMutableContext(t.Context(), root), ComponentRef{})
	require.NoError(t, err)
	mutation := closeDataMap(t, root)
	require.NotContains(t, mutation.UpdatedNodes, "Visibility")
	require.Equal(t, 2, calls.count)
	ctx := NewMutableContext(t.Context(), root)
	value.Content.Get(ctx).Data = []byte("changed")
	mutation = closeDataMap(t, root)
	require.Contains(t, mutation.UpdatedNodes, "Visibility")
	require.Equal(t, 3, calls.count)
	fresh := reloadMemoTree(t, root, memoSnapshot(root))
	require.NotNil(t, fresh.currentMemo)
	require.Equal(t, 4, calls.count)
}

type memoVersionOnlyFixture struct {
	UnimplementedComponent
	State *commonpb.Payload
}

func (*memoVersionOnlyFixture) LifecycleState(Context) LifecycleState { return LifecycleStateRunning }
func (*memoVersionOnlyFixture) MemoVersion(Context) uint64            { return 1 }

type memoVersionOnlyLibrary struct{ UnimplementedLibrary }

func (*memoVersionOnlyLibrary) Name() string { return "versiononly" }
func (*memoVersionOnlyLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{NewRegistrableComponent[*memoVersionOnlyFixture]("root")}
}
func TestVisibilityMemoVersionRegistrationAndColdZero(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	require.ErrorContains(t, NewRegistry(logger).Register(&memoVersionOnlyLibrary{}), "without Memo")
	root, _, calls := newMemoVersionTree(t, false)
	saved := memoSnapshot(root)
	var err error
	saved[""].Data, err = encodeChasmBlob(&commonpb.Payload{Data: make([]byte, 8)})
	require.NoError(t, err)
	_, err = NewTreeFromDB(saved, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "nonzero")
	require.Zero(t, calls.count)
}
