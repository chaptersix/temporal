//go:build test_dep

package chasm

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

// DataMap removes collection-wide hydration and synchronization, but the default
// tree still traverses unrelated nodes during close. This is not the worklist gate.
func TestDataMapDefaultTraversalWork(t *testing.T) {
	t.Parallel()
	for _, size := range []int{4, 5, 1000} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			root, value, ctx := newDataMapFixture(t)
			for i := range size {
				value.Records.Put(ctx, strconv.Itoa(i), &commonpb.Payload{Data: []byte("unchanged")})
			}
			require.NoError(t, root.SetRootComponent(value))
			closeDataMap(t, root)
			saved := memoSnapshot(root)
			counts := root.observation.counters
			counts.Reset()
			fresh, err := NewTreeFromDB(saved, root.registry, root.backend, DefaultPathEncoder, root.logger, root.metricsHandler)
			require.NoError(t, err)
			cold := counts.Snapshot()
			require.EqualValues(t, len(saved), cold["cold_nodes"])
			require.Zero(t, cold["map_hydration_keys"])
			require.Zero(t, cold["data_decode_calls"])
			emitWorkCounts(t, size, "datamap-cold", cold)
			for step := range 2 {
				counts.Reset()
				current := NewMutableContext(t.Context(), fresh)
				component, err := fresh.Component(current, ComponentRef{})
				require.NoError(t, err)
				value := component.(*dataMapFixture)
				item, ok := value.Records.Get(current, "0")
				require.True(t, ok)
				item.Data = []byte(strconv.Itoa(step))
				value.Records.Put(current, "0", item)
				mutation := closeDataMap(t, fresh)
				require.LessOrEqual(t, len(mutation.UpdatedNodes), 2)
				key := "Records#0"
				require.Contains(t, mutation.UpdatedNodes, key)
				for changed := range mutation.UpdatedNodes {
					require.True(t, changed == "" || changed == key, "unrelated DATA must not be serialized")
				}
				require.Empty(t, mutation.DeletedNodes)
				hot := counts.Snapshot()
				require.Zero(t, hot["cold_nodes"])
				require.Zero(t, hot["map_hydration_keys"])
				require.Zero(t, hot["map_sync_keys"])
				require.Zero(t, hot["map_key_copies"])
				require.EqualValues(t, 1-step, hot["data_decode_calls"], "only the addressed value is decoded, once after reload")
				require.Greater(t, hot["tree_visits"], int64(size), "default close still inspects unrelated metadata")
				emitWorkCounts(t, size, "datamap-hot"+strconv.Itoa(step), hot)
			}
		})
	}
}
