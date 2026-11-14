//go:build test_dep

package scheduler2

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/packedstore"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

type mutableQueueIndexAdapter struct{ backend *mutableStorageBackend }

func (a *mutableQueueIndexAdapter) Read(key string) ([]byte, bool, error) {
	b := packedstore.NewBatch(a.backend)
	defer b.Abort()
	return b.Get(packedstore.ID{'X'}, []byte(key))
}
func (a *mutableQueueIndexAdapter) Apply(changes []queueindex.IndexChange) error {
	b := packedstore.NewBatch(a.backend)
	defer b.Abort()
	for _, c := range changes {
		var err error
		if c.Delete {
			err = b.Delete(packedstore.ID{'X'}, []byte(c.Key))
		} else {
			err = b.Put(packedstore.ID{'X'}, []byte(c.Key), c.Value)
		}
		if err != nil {
			return err
		}
	}
	return b.Commit()
}

// Measures actual mutable queueindex batches through the packed backend. Queue
// record payloads and complete Scheduler controls are excluded and clearly reported.
func TestMutablePackedSixClassIndexMeasurement(t *testing.T) {
	sizes := []int{4}
	if os.Getenv("SCHEDULER_COMPACT_LARGE") == "1" {
		sizes = []int{1000, 10000}
	}
	for _, n := range sizes {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			h, wire := wholeRuntimeTree(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			wholeRuntimeDue(t, h, wire)
			_, _, _, exemplar := mutableStorageRows(t, h)
			backend := &mutableStorageBackend{data: map[string][]byte{}, exemplar: exemplar}
			store := queueindex.NewCountStoreWithBackend(&mutableQueueIndexAdapter{backend: backend})
			index := func(generation uint64, class int) queueindex.SequenceIndex {
				var id queueindex.SequenceIndex
				binary.BigEndian.PutUint64(id[:8], generation)
				id[8] = byte(class)
				return id
			}
			at := func(i int) queueindex.CountKey {
				var k queueindex.CountKey
				binary.BigEndian.PutUint64(k[:8], uint64(i)*11400714819323198485)
				binary.BigEndian.PutUint32(k[8:], uint32(i%1000000000))
				return k
			}
			for i := range n {
				b := store.Begin()
				require.NoError(t, b.InsertSequence(index(1, i%6), uint64(i+1), at(i)))
				require.NoError(t, b.Commit())
			}
			initial := backend.framed
			for i := range n / 2 {
				b := store.Begin()
				require.NoError(t, b.DeleteSequence(index(1, i%6), uint64(i+1)))
				require.NoError(t, b.Commit())
			}
			for i := range max(1, n/8) {
				b := store.Begin()
				require.NoError(t, b.InsertSequence(index(2, i%6), uint64(n+i+1), at(n+i)))
				require.NoError(t, b.Commit())
			}
			remaining := uint64(0)
			b := store.Begin()
			defer b.Abort()
			for class := range 6 {
				count, err := b.SequenceCount(index(1, class), 0, ^uint64(0), queueindex.CountKey{})
				require.NoError(t, err)
				remaining += count
			}
			require.EqualValues(t, n-n/2, remaining)
			t.Logf("MUTABLE_SIX_CLASS_INDEX_ONLY N=%d initial_framed=%d two_generation_framed=%d peak_committed=%d nodes=%d max_mutation_updated_framed_bytes=%d max_changed_pages=%d max_backend_reads=%d; excludes queue records and Scheduler", n, initial, backend.framed, backend.peak, len(backend.data), backend.maxBatchBytes, backend.maxChanges, backend.maxReads)
		})
	}
}
