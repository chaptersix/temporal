//go:build test_dep

package chasm

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

type requiredEmptyFixture struct {
	UnimplementedComponent
	State   *commonpb.Payload
	Legacy  Map[string, *commonpb.Payload] `name:"old"`
	Records DataMap[*commonpb.Payload]
	poison  string
}

func (v *requiredEmptyFixture) LifecycleState(Context) LifecycleState {
	if v.poison == "lifecycle" {
		v.Legacy = Map[string, *commonpb.Payload]{"bad": {}}
	}
	return LifecycleStateRunning
}
func (v *requiredEmptyFixture) IsExecutionSkippable(Context) bool {
	if v.poison == "time skipping" {
		v.Legacy = Map[string, *commonpb.Payload]{"bad": {}}
	}
	return false
}
func (*requiredEmptyFixture) ContextMetadata(Context) map[string]string { return nil }
func (*requiredEmptyFixture) Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error) {
	return TerminateComponentResponse{}, nil
}

type requiredEmptyLibrary struct {
	UnimplementedLibrary
	names   []string
	bounded bool
}

func (*requiredEmptyLibrary) Name() string { return "requiredempty" }
func (l *requiredEmptyLibrary) Components() []*RegistrableComponent {
	var opts []RegistrableComponentOption
	if l.bounded {
		opts = append(opts, WithBoundedDataLayout(BoundedDataLayout{Components: []BoundedComponentSlot{BoundedComponent[*requiredEmptyFixture](nil, BoundedTaskLimits{}).WithRequiredEmptyMaps(l.names...)}}))
	}
	return []*RegistrableComponent{NewRegistrableComponent[*requiredEmptyFixture]("root", opts...)}
}
func requiredEmptyTree(t *testing.T, bounded bool) (*Node, *requiredEmptyFixture, MutableContext) {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	registry := NewRegistry(logger)
	require.NoError(t, registry.Register(&CoreLibrary{}))
	require.NoError(t, registry.Register(&requiredEmptyLibrary{names: []string{"old"}, bounded: bounded}))
	backend := &MockNodeBackend{HandleGetCurrentVersion: func() int64 { return 1 }, HandleNextTransitionCount: func() int64 { return 1 }}
	root := NewEmptyTree(registry, backend, DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := NewMutableContext(t.Context(), root)
	return root, &requiredEmptyFixture{State: &commonpb.Payload{}, Records: NewDataMap[*commonpb.Payload](ctx)}, ctx
}
func TestRequiredEmptyMapDeclaration(t *testing.T) {
	t.Parallel()
	for _, names := range [][]string{nil, {""}, {"missing"}, {"Legacy"}, {"State"}, {"Records"}, {"old", "old"}} {
		t.Run("names="+fmt.Sprint(names), func(t *testing.T) {
			registry := NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
			require.Error(t, registry.Register(&requiredEmptyLibrary{names: names, bounded: true}))
		})
	}
	for _, typ := range []reflect.Type{
		reflect.TypeFor[*struct {
			Children Map[string, *requiredEmptyFixture]
		}](),
		reflect.TypeFor[*struct{ Children Map[string, proto.Message] }](),
		reflect.TypeFor[*struct{ Children Map[string, string] }](),
	} {
		require.Error(t, validateEmptyMapDeclaration(BoundedComponentSlot{typ: typ, emptyMaps: []string{"Children"}}))
	}
	require.Error(t, validateEmptyMapDeclaration(BoundedComponentSlot{typ: reflect.TypeFor[*struct {
		First  Map[string, *commonpb.Payload]     `name:"old"`
		Second Map[string, *requiredEmptyFixture] `name:"old"`
	}](), emptyMaps: []string{"old"}}))
	names := []string{"old"}
	slot := BoundedComponent[*requiredEmptyFixture](nil, BoundedTaskLimits{}).WithRequiredEmptyMaps(names...)
	names[0] = "changed"
	rc := NewRegistrableComponent[*requiredEmptyFixture]("root", WithBoundedDataLayout(BoundedDataLayout{Components: []BoundedComponentSlot{slot}}))
	slot.emptyMaps[0] = "changed again"
	_, err := compileBoundedLayout(rc)
	require.NoError(t, err)
}
func TestRequiredEmptyMapRejectsHotEntries(t *testing.T) {
	t.Parallel()
	for _, size := range []int{1, 10000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			root, value, _ := requiredEmptyTree(t, true)
			require.NoError(t, root.SetRootComponent(value))
			closeDataMap(t, root)
			saved := memoSnapshot(root)
			counts := root.BeginWorkInspectionForTest()
			ctx := NewMutableContext(t.Context(), root)
			value.Legacy = make(Map[string, *commonpb.Payload], size)
			for i := range size {
				value.Legacy[fmt.Sprint(i)] = NewDataField(ctx, &commonpb.Payload{})
			}
			mutation, err := root.CloseTransaction(t.Context(), true)
			require.ErrorContains(t, err, "required-empty map")
			require.Empty(t, mutation.UpdatedNodes)
			require.Empty(t, mutation.DeletedNodes)
			require.Zero(t, counts.Snapshot()["map_key_copies"])
			require.Zero(t, counts.Snapshot()["map_hydration_keys"])
			fresh := reloadMemoTree(t, root, saved)
			component, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
			require.NoError(t, err)
			require.Empty(t, component.(*requiredEmptyFixture).Legacy)
		})
	}
}
func TestRequiredEmptyMapAttachmentAndDefault(t *testing.T) {
	t.Parallel()
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprint(bounded), func(t *testing.T) {
			root, value, ctx := requiredEmptyTree(t, bounded)
			value.Legacy = Map[string, *commonpb.Payload]{"one": NewDataField(ctx, &commonpb.Payload{})}
			err := root.SetRootComponent(value)
			if bounded {
				require.ErrorContains(t, err, "required-empty map")
				return
			}
			require.NoError(t, err)
			mutation := closeDataMap(t, root)
			require.Contains(t, mutation.UpdatedNodes, "old#one")
		})
	}
}
func TestRequiredEmptyMapColdAndReplication(t *testing.T) {
	t.Parallel()
	old, value, ctx := requiredEmptyTree(t, false)
	value.Legacy = Map[string, *commonpb.Payload]{"one": NewDataField(ctx, &commonpb.Payload{})}
	require.NoError(t, old.SetRootComponent(value))
	closeDataMap(t, old)
	saved := memoSnapshot(old)
	root, current, _ := requiredEmptyTree(t, true)
	require.NoError(t, root.SetRootComponent(current))
	closeDataMap(t, root)
	_, err := NewTreeFromDB(saved, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "required-empty map")
	// Empty legacy containers are compatible, but no entry can be replicated in.
	delete(saved, "old#one")
	fresh, err := NewTreeFromDB(saved, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	incoming := proto.Clone(old.children["old"].children["one"].serializedNode).(*persistencespb.ChasmNode)
	incoming.Metadata.LastUpdateVersionedTransition.TransitionCount++
	require.Error(t, fresh.ApplyMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{"old#one": incoming}}))
	fresh, err = NewTreeFromDB(saved, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	require.Error(t, fresh.ApplySystemMutation(NodesMutation{UpdatedNodes: map[string]*persistencespb.ChasmNode{"old#one": incoming}}))
	// A DATA value cannot masquerade as the required-empty collection itself.
	saved["old"] = proto.Clone(incoming).(*persistencespb.ChasmNode)
	_, err = NewTreeFromDB(saved, root.registry, root.backend, DefaultPathEncoder, root.logger, metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "required-empty map")
}

func TestRequiredEmptyMapRejectsLateCallbackMutation(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"lifecycle", "time skipping"} {
		t.Run(phase, func(t *testing.T) {
			root, value, _ := requiredEmptyTree(t, true)
			require.NoError(t, root.SetRootComponent(value))
			closeDataMap(t, root)
			saved := memoSnapshot(root)
			backend := root.backend.(*MockNodeBackend)
			backend.HandleGetExecutionInfo = func() *persistencespb.WorkflowExecutionInfo {
				return &persistencespb.WorkflowExecutionInfo{TimeSkippingInfo: &persistencespb.TimeSkippingInfo{Config: &commonpb.TimeSkippingConfig{Enabled: true}}}
			}
			backend.HandleIsTimeSkippingFeatureEnabled = func(ArchetypeID) bool { return true }
			_, err := root.Component(NewMutableContext(t.Context(), root), ComponentRef{})
			require.NoError(t, err)
			value.poison = phase
			mutation, err := root.CloseTransaction(t.Context(), true)
			require.ErrorContains(t, err, "required-empty map")
			require.NotEmpty(t, value.Legacy, "malicious callback must have actually executed")
			require.Empty(t, mutation.UpdatedNodes)
			require.Empty(t, mutation.DeletedNodes)
			fresh := reloadMemoTree(t, root, saved)
			current, err := fresh.Component(NewContext(t.Context(), fresh), ComponentRef{})
			require.NoError(t, err)
			require.Empty(t, current.(*requiredEmptyFixture).Legacy)
		})
	}
}
