package queueindex

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func sequenceLowerReference(records map[uint64]CountKey, lower uint64) (SequenceEntry, bool) {
	var best SequenceEntry
	found := false
	for sequence, at := range records {
		if sequence >= lower && (!found || sequence < best.Sequence) {
			best = SequenceEntry{Sequence: sequence, Time: at}
			found = true
		}
	}
	return best, found
}

func TestSequenceAtOrAfterReference(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		store := NewCountStore()
		index := SequenceIndex{51}
		records := map[uint64]CountKey{}
		for range 50 {
			sequence := rapid.Uint64().Draw(rt, "sequence")
			at := countTestKey(sequence % 17)
			b := store.Begin()
			if _, exists := records[sequence]; exists {
				require.NoError(rt, b.DeleteSequence(index, sequence))
				delete(records, sequence)
			} else {
				require.NoError(rt, b.InsertSequence(index, sequence, at))
				records[sequence] = at
			}
			for _, lower := range []uint64{0, sequence, rapid.Uint64().Draw(rt, "lower"), math.MaxUint64} {
				want, present := sequenceLowerReference(records, lower)
				got, ok, err := b.SequenceAtOrAfter(index, lower)
				require.NoError(rt, err)
				require.Equal(rt, present, ok)
				require.Equal(rt, want, got)
			}
			require.NoError(rt, b.Commit())
		}
		before := backendSnapshot(store.backend.(*MemoryIndexBackend))
		b := store.Begin()
		for _, lower := range []uint64{0, rapid.Uint64().Draw(rt, "persisted lower"), math.MaxUint64} {
			want, present := sequenceLowerReference(records, lower)
			oldReads := b.Stats().Reads
			got, ok, err := b.SequenceAtOrAfter(index, lower)
			require.NoError(rt, err)
			require.Equal(rt, present, ok)
			require.Equal(rt, want, got)
			require.LessOrEqual(rt, b.Stats().Reads-oldReads, 258)
		}
		require.Empty(rt, b.extra)
		require.Empty(rt, b.tx.changed)
		b.Abort()
		require.Equal(rt, before, store.backend.(*MemoryIndexBackend).values)
	})
}

func TestSequenceAtOrAfterBoundariesAndReadFailures(t *testing.T) {
	t.Parallel()
	memory := NewMemoryIndexBackend()
	store := NewCountStoreWithBackend(memory)
	index := SequenceIndex{52}
	b := store.Begin()
	_, ok, err := b.SequenceAtOrAfter(index, 0)
	require.NoError(t, err)
	require.False(t, ok)
	for _, sequence := range []uint64{0, 1, 3, 8, 16, 31, 64, 128, math.MaxUint64} {
		require.NoError(t, b.InsertSequence(index, sequence, countTestKey(sequence%5)))
	}
	require.NoError(t, b.Commit())
	for _, lower := range []uint64{0, 1, 2, 4, 9, 17, 32, 65, 129, math.MaxUint64} {
		counter := &faultIndexBackend{memory: &MemoryIndexBackend{values: backendSnapshot(memory)}}
		query := NewCountStoreWithBackend(counter).Begin()
		entry, ok, err := query.SequenceAtOrAfter(index, lower)
		require.NoError(t, err)
		require.True(t, ok)
		require.GreaterOrEqual(t, entry.Sequence, lower)
		query.Abort()
		for fault := 1; fault <= counter.reads; fault++ {
			backend := &faultIndexBackend{memory: &MemoryIndexBackend{values: backendSnapshot(memory)}, failRead: fault}
			query := NewCountStoreWithBackend(backend).Begin()
			_, _, err := query.SequenceAtOrAfter(index, lower)
			require.ErrorContains(t, err, "injected read failure")
			require.Error(t, query.Commit())
			require.Equal(t, memory.values, backend.memory.values)
		}
	}
}
