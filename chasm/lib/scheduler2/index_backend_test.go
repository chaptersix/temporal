package scheduler2

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

type indexTestRoot struct {
	chasm.UnimplementedComponent
	State *commonpb.Payload
	Pages chasm.DataMap[*commonpb.Payload]
}

func (*indexTestRoot) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
func (*indexTestRoot) ContextMetadata(chasm.Context) map[string]string { return nil }
func (*indexTestRoot) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

type indexTestLibrary struct{ chasm.UnimplementedLibrary }

func (*indexTestLibrary) Name() string { return "schedulerindexfixture" }
func (*indexTestLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{chasm.NewRegistrableComponent[*indexTestRoot]("root", chasm.WithBoundedDataLayout(chasm.BoundedDataLayout{Components: []chasm.BoundedComponentSlot{chasm.BoundedComponent[*indexTestRoot](nil, chasm.BoundedTaskLimits{})}}))}
}
func newIndexHarness(t *testing.T) *treeHarness {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&indexTestLibrary{}))
	h := &treeHarness{t: t, registry: registry, transition: 1}
	h.resetBackend()
	h.node = chasm.NewEmptyTree(registry, h.backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(t.Context(), h.node)
	require.NoError(t, h.node.SetRootComponent(&indexTestRoot{State: &commonpb.Payload{}, Pages: chasm.NewDataMap[*commonpb.Payload](ctx)}))
	h.commit(true)
	return h
}
func indexHarnessBackend(t *testing.T, h *treeHarness, mutable bool) *indexBackend {
	t.Helper()
	ctx := chasm.NewContext(t.Context(), h.node)
	if mutable {
		ctx = chasm.NewMutableContext(t.Context(), h.node)
	}
	root, err := h.node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	return &indexBackend{ctx: ctx, pages: root.(*indexTestRoot).Pages}
}
func indexTimeKey(value uint64) queueindex.CountKey {
	var key queueindex.CountKey
	binary.BigEndian.PutUint64(key[:8], value)
	return key
}
func TestIndexBackendPublicationAndReload(t *testing.T) {
	t.Parallel()
	h := newIndexHarness(t)
	index := queueindex.SequenceIndex{1}
	for _, publish := range []bool{false, true} {
		b := queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, true)).Begin()
		for _, seq := range []uint64{1, 2, 3} {
			require.NoError(t, b.InsertSequence(index, seq, indexTimeKey(seq)))
		}
		require.NoError(t, b.Commit())
		h.commit(publish)
		query := queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, false)).Begin()
		count, err := query.SequenceCount(index, 0, math.MaxUint64, queueindex.CountKey{})
		require.NoError(t, err)
		expected := uint64(0)
		if publish {
			expected = 3
		}
		require.Equal(t, expected, count)
		query.Abort()
	}
	b := queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, true)).Begin()
	require.NoError(t, b.DeleteSequence(index, 2))
	require.NoError(t, b.Commit())
	h.commit(true)
	query := queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, false)).Begin()
	defer query.Abort()
	count, err := query.SequenceCount(index, 0, math.MaxUint64, indexTimeKey(2))
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.ErrorContains(t, indexHarnessBackend(t, h, false).Apply(nil), "mutable context")
}

// This measures the complete CHASM node protobufs and encoded paths. Database
// row overhead, WorkflowMutableState wrapper fields and Go heap are excluded.
func TestIndexBackendSerializedFootprint(t *testing.T) {
	t.Parallel()
	for _, n := range []int{4, 5, 1000, 10000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			h := newIndexHarness(t)
			index := queueindex.SequenceIndex{3}
			store := queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, true))
			for i := 0; i < n; i++ {
				batch := store.Begin()
				require.NoError(t, batch.InsertSequence(index, uint64(i), indexTimeKey(uint64(i)*11400714819323198485)))
				require.NoError(t, batch.Commit())
			}
			h.commit(true)
			nodeBytes, pathBytes := 0, 0
			for path, node := range h.durable {
				nodeBytes += proto.Size(node)
				pathBytes += len(path)
			}
			t.Logf("N=%d nodes=%d node_protobuf_bytes=%d path_bytes=%d total=%d; excludes database rows/whole-record wrapper/heap", n, len(h.durable), nodeBytes, pathBytes, nodeBytes+pathBytes)
			// Verify the populated index remains addressable after full serialization/reload.
			batch := queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, true)).Begin()
			require.NoError(t, batch.InsertSequence(index, uint64(n), indexTimeKey(uint64(n)*11400714819323198485)))
			require.NoError(t, batch.Commit())
			mutation := h.commit(true)
			changedBytes := 0
			for path, node := range mutation.UpdatedNodes {
				changedBytes += len(path) + proto.Size(node)
			}
			t.Logf("N=%d next_point_updated_nodes=%d deleted_nodes=%d changed_node_and_path_bytes=%d", n, len(mutation.UpdatedNodes), len(mutation.DeletedNodes), changedBytes)
			query := queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, false)).Begin()
			defer query.Abort()
			count, err := query.SequenceCount(index, 0, math.MaxUint64, queueindex.CountKey{})
			require.NoError(t, err)
			require.EqualValues(t, n+1, count)
		})
	}
}
