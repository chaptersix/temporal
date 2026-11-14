//go:build test_dep

package scheduler2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

func TestIndexBackendHotWorkIsolation(t *testing.T) {
	t.Parallel()
	var baseline map[string]int64
	for _, n := range []int{4, 5, 1000} {
		// Compare complete counter vectors serially; each fixture is otherwise isolated.
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			h := newIndexHarness(t)
			backend := indexHarnessBackend(t, h, true)
			changes := make([]queueindex.IndexChange, n)
			for i := range n {
				changes[i] = queueindex.IndexChange{Key: fmt.Sprintf("noise%d", i), Value: []byte("unrelated")}
			}
			require.NoError(t, backend.Apply(changes))
			index := queueindex.SequenceIndex{1}
			batch := queueindex.NewCountStoreWithBackend(backend).Begin()
			require.NoError(t, batch.InsertSequence(index, 1, indexTimeKey(100)))
			require.NoError(t, batch.Commit())
			h.commit(true)
			// The unrelated page is not a valid protobuf. Point work must leave it unread.
			poison := h.durable["Pages#noise0"]
			require.NotNil(t, poison)
			poison.Data.Data = []byte{0xff}
			h.reload()
			inspection := h.node.BeginWorkInspectionForTest()
			batch = queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, true)).Begin()
			require.NoError(t, batch.InsertSequence(index, 2, indexTimeKey(99)))
			require.NoError(t, batch.Commit())
			mutation, err := h.node.CloseTransaction(t.Context(), true)
			require.NoError(t, err)
			measured := inspection.Snapshot() // Exclude full snapshot/export and cold reload.
			for _, key := range []string{"tree_visits", "map_hydration_keys", "map_sync_keys", "map_key_copies", "cold_nodes"} {
				require.Zero(t, measured[key], key)
			}
			if baseline == nil {
				baseline = measured
			} else {
				require.Equal(t, baseline, measured)
			}
			t.Logf("N=%d hot_counts=%v", n, measured)
			// Publish only the mutation, then reconstruct to check the changed index.
			for key, node := range mutation.UpdatedNodes {
				h.durable[key] = node
			}
			for key := range mutation.DeletedNodes {
				delete(h.durable, key)
			}
			h.transition++
			h.reload()
			query := queueindex.NewCountStoreWithBackend(indexHarnessBackend(t, h, false)).Begin()
			defer query.Abort()
			count, err := query.SequenceCount(index, 0, 2, queueindex.CountKey{})
			require.NoError(t, err)
			require.EqualValues(t, 2, count)
			require.Equal(t, []byte{0xff}, h.durable["Pages#noise0"].Data.Data)
		})
	}
}
