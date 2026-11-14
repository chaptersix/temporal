package queueindex

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func earliestReference(records map[uint64]CountKey) (SequenceEntry, bool) {
	var result SequenceEntry
	found := false
	for sequence, at := range records {
		cmp := bytes.Compare(at[:], result.Time[:])
		if !found || cmp < 0 || cmp == 0 && sequence < result.Sequence {
			result = SequenceEntry{Sequence: sequence, Time: at}
			found = true
		}
	}
	return result, found
}

func TestSequenceEarliestReference(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		store := NewCountStore()
		index := SequenceIndex{31}
		model := make(map[uint64]CountKey)
		for range 80 {
			sequence := rapid.Uint64Range(0, 31).Draw(rt, "sequence")
			at := countTestKey(rapid.Uint64Range(0, 15).Draw(rt, "time"))
			batch := store.Begin()
			if _, ok := model[sequence]; ok {
				require.NoError(rt, batch.DeleteSequence(index, sequence))
				delete(model, sequence)
			} else {
				require.NoError(rt, batch.InsertSequence(index, sequence, at))
				model[sequence] = at
			}
			// The query must observe the transaction overlay, not stale backing pages.
			expected, present := earliestReference(model)
			actual, ok, err := batch.SequenceEarliest(index)
			require.NoError(rt, err)
			require.Equal(rt, present, ok)
			require.Equal(rt, expected, actual)
			require.NoError(rt, batch.Commit())
			before := backendSnapshot(store.backend.(*MemoryIndexBackend))
			query := store.Begin()
			actual, ok, err = query.SequenceEarliest(index)
			require.NoError(rt, err)
			require.Equal(rt, present, ok)
			require.Equal(rt, expected, actual)
			require.LessOrEqual(rt, query.Stats().Reads, 65*800)
			require.Zero(rt, query.Stats().Writes)
			require.Zero(rt, query.Stats().Releases)
			require.Zero(rt, query.Stats().EncodedBytes)
			require.Empty(rt, query.tx.changed)
			require.Empty(rt, query.extra)
			query.Abort()
			require.Equal(rt, before, store.backend.(*MemoryIndexBackend).values)
			requireSequenceStore(rt, store, index, model)
		}
	})
}

func TestSequenceEarliestTimeThenSequence(t *testing.T) {
	t.Parallel()
	store := NewCountStore()
	index := SequenceIndex{32}
	records := map[uint64]CountKey{0: countTestKey(20), 5: countTestKey(10), 7: countTestKey(10), math.MaxUint64: CountKey{}}
	for sequence, at := range records {
		b := store.Begin()
		require.NoError(t, b.InsertSequence(index, sequence, at))
		require.NoError(t, b.Commit())
	}
	for _, sequence := range []uint64{math.MaxUint64, 5, 7, 0} {
		b := store.Begin()
		entry, ok, err := b.SequenceEarliest(index)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, sequence, entry.Sequence)
		require.NoError(t, b.DeleteSequence(index, sequence))
		require.NoError(t, b.Commit())
	}
	b := store.Begin()
	defer b.Abort()
	_, ok, err := b.SequenceEarliest(index)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSequenceEarliestReadFailuresPoisonBatch(t *testing.T) {
	t.Parallel()
	memory := NewMemoryIndexBackend()
	store := NewCountStoreWithBackend(memory)
	index := SequenceIndex{33}
	for i := uint64(0); i < 40; i++ {
		b := store.Begin()
		require.NoError(t, b.InsertSequence(index, i, countTestKey(40-i)))
		require.NoError(t, b.Commit())
	}
	before := backendSnapshot(memory)
	counter := &faultIndexBackend{memory: &MemoryIndexBackend{values: backendSnapshot(memory)}}
	b := NewCountStoreWithBackend(counter).Begin()
	_, ok, err := b.SequenceEarliest(index)
	require.NoError(t, err)
	require.True(t, ok)
	b.Abort()
	for fault := 1; fault <= counter.reads; fault++ {
		backend := &faultIndexBackend{memory: &MemoryIndexBackend{values: backendSnapshot(memory)}, failRead: fault}
		b := NewCountStoreWithBackend(backend).Begin()
		_, _, err := b.SequenceEarliest(index)
		require.ErrorContains(t, err, "injected read failure")
		reads := backend.reads
		require.ErrorContains(t, b.Commit(), "injected read failure")
		require.Equal(t, reads, backend.reads)
		require.Zero(t, backend.applies)
		require.Equal(t, before, backend.memory.values)
	}
}

func TestSequenceEarliestRejectsChronologicalSummaryMismatch(t *testing.T) {
	t.Parallel()
	store := NewCountStore()
	index := SequenceIndex{34}
	b := store.Begin()
	require.NoError(t, b.InsertSequence(index, 1, countTestKey(10)))
	require.NoError(t, b.InsertSequence(index, 2, countTestKey(20)))
	require.NoError(t, b.Commit())
	b = store.Begin()
	root, err := b.sequenceRoot(index)
	require.NoError(t, err)
	node, err := b.sequenceNode(index, root)
	require.NoError(t, err)
	b.Abort()
	// A well-encoded inline leaf can still contradict its parent's separately
	// valid immutable summary. The minimum query must detect that relationship.
	leaf := sequenceNode{Bit: 64, Min: 1, Time: countTestKey(30)}
	require.NoError(t, store.backend.Apply([]IndexChange{{Key: index.pageKey(node.Left), Value: leaf.encode()}}))
	before := backendSnapshot(store.backend.(*MemoryIndexBackend))
	b = store.Begin()
	_, _, err = b.SequenceEarliest(index)
	require.ErrorContains(t, err, "minimum mismatch")
	require.Error(t, b.Commit())
	require.Equal(t, before, store.backend.(*MemoryIndexBackend).values)
}

func TestSequenceEarliestFullDepthAndSharedOwnership(t *testing.T) {
	t.Parallel()
	store := NewCountStore()
	index := SequenceIndex{35}
	owner, _, err := store.Update(CountRoot{}, CountKey{}, true)
	require.NoError(t, err)
	for bit := range 64 {
		b := store.Begin()
		require.NoError(t, b.InsertSequence(index, uint64(1)<<bit, countTestKey(uint64(64-bit))))
		require.NoError(t, b.Commit())
	}
	b := store.Begin()
	require.NoError(t, b.InsertSequence(index, 0, CountKey{}))
	require.NoError(t, b.Commit())
	before := backendSnapshot(store.backend.(*MemoryIndexBackend))
	b = store.Begin()
	entry, ok, err := b.SequenceEarliest(index)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, SequenceEntry{}, entry)
	require.LessOrEqual(t, b.Stats().Reads, 65*800)
	t.Logf("full sequence depth earliest reads=%d", b.Stats().Reads)
	b.Abort()
	require.Equal(t, before, store.backend.(*MemoryIndexBackend).values)
	count, err := store.Count(owner)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestSequenceEarliestSignedAndWideTimes(t *testing.T) {
	t.Parallel()
	times := []time.Time{time.Unix(0, math.MaxInt64), time.Unix(0, 0), time.Unix(0, math.MinInt64), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)}
	store := NewCountStore()
	index := SequenceIndex{36}
	for i, at := range times {
		full := NewTimeKey(at, 0)
		b := store.Begin()
		require.NoError(t, b.InsertSequence(index, uint64(i+1), CountKey(full[:12])))
		require.NoError(t, b.Commit())
	}
	for _, sequence := range []uint64{4, 3, 2, 1, 5} {
		b := store.Begin()
		entry, ok, err := b.SequenceEarliest(index)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, sequence, entry.Sequence)
		require.NoError(t, b.DeleteSequence(index, sequence))
		require.NoError(t, b.Commit())
	}
}

func TestSequenceEarliestMissingMinimumPage(t *testing.T) {
	t.Parallel()
	store := NewCountStore()
	index := SequenceIndex{37}
	for i := uint64(0); i < 40; i++ {
		b := store.Begin()
		require.NoError(t, b.InsertSequence(index, i, countTestKey(i)))
		require.NoError(t, b.Commit())
	}
	b := store.Begin()
	root, err := b.sequenceRoot(index)
	require.NoError(t, err)
	node, err := b.sequenceNode(index, root)
	require.NoError(t, err)
	entry, err := b.tx.read(node.Summary)
	require.NoError(t, err)
	require.Less(t, entry.Node.Bit, uint8(96))
	b.Abort()
	require.NoError(t, store.backend.Apply([]IndexChange{{Key: countPageKey(entry.Node.Left), Delete: true}}))
	before := backendSnapshot(store.backend.(*MemoryIndexBackend))
	b = store.Begin()
	_, _, err = b.SequenceEarliest(index)
	require.ErrorContains(t, err, "missing count node")
	require.Error(t, b.Commit())
	require.Equal(t, before, store.backend.(*MemoryIndexBackend).values)
}

func TestSequenceEarliestCombinedKeyDepth(t *testing.T) {
	t.Parallel()
	store := NewCountStore()
	index := SequenceIndex{38}
	b := store.Begin()
	require.NoError(t, b.InsertSequence(index, 0, CountKey{}))
	require.NoError(t, b.Commit())
	sequences := make([]uint64, 0, 96)
	for bit := range 64 {
		sequences = append(sequences, uint64(1)<<bit)
	}
	for i := range 32 {
		sequences = append(sequences, uint64(3+2*i))
	}
	for i, sequence := range sequences {
		var at CountKey
		at[i/8] = 1 << uint(7-i%8)
		b = store.Begin()
		require.NoError(t, b.InsertSequence(index, sequence, at))
		require.NoError(t, b.Commit())
	}
	b = store.Begin()
	defer b.Abort()
	actual, ok, err := b.SequenceEarliest(index)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, SequenceEntry{}, actual)
	require.LessOrEqual(t, b.Stats().Reads, 65*800)
	t.Logf("combined64/96-bit depth earliest reads=%d", b.Stats().Reads)
}

func TestSequenceEarliestSharedGenerationSummary(t *testing.T) {
	t.Parallel()
	store := NewCountStore()
	indexes := [2]SequenceIndex{{39}, {40}}
	for _, index := range indexes {
		for i := uint64(0); i < 40; i++ {
			b := store.Begin()
			require.NoError(t, b.InsertSequence(index, i, countTestKey(40-i)))
			require.NoError(t, b.Commit())
		}
	}
	b := store.Begin()
	var roots [2]CountRoot
	for i, index := range indexes {
		id, err := b.sequenceRoot(index)
		require.NoError(t, err)
		node, err := b.sequenceNode(index, id)
		require.NoError(t, err)
		roots[i] = node.Summary
	}
	require.Equal(t, roots[0], roots[1])
	shared, err := b.tx.read(roots[0])
	require.NoError(t, err)
	require.GreaterOrEqual(t, shared.Refs, uint64(2))
	b.Abort()
	before := backendSnapshot(store.backend.(*MemoryIndexBackend))
	for _, index := range indexes {
		b = store.Begin()
		entry, ok, err := b.SequenceEarliest(index)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, SequenceEntry{Sequence: 39, Time: countTestKey(1)}, entry)
		require.Empty(t, b.tx.changed)
		b.Abort()
	}
	require.Equal(t, before, store.backend.(*MemoryIndexBackend).values)
}
