package queueindex

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func requireSequenceStore(t require.TestingT, s *CountStore, index SequenceIndex, model map[uint64]CountKey) {
	b := s.Begin()
	defer b.Abort()
	memory := s.backend.(*MemoryIndexBackend)
	var roots []CountRoot
	pages := 0
	for key := range memory.values {
		if !strings.HasPrefix(key, "sequence/") {
			continue
		}
		raw, err := base64.RawURLEncoding.DecodeString(key[strings.LastIndex(key, "/")+1:])
		require.NoError(t, err)
		require.Len(t, raw, 9)
		var id sequenceID
		copy(id[:], raw)
		node, err := b.sequenceNode(index, id)
		require.NoError(t, err)
		roots = append(roots, node.Summary)
		pages++
		// Each secondary summary must equal the independent leaf set under its prefix.
		for _, threshold := range []CountKey{{}, countTestKey(8), countTestKey(16), countTestKey(32)} {
			var want uint64
			for seq, at := range model {
				if sequenceKey(node.Bit, seq) == id && bytes.Compare(at[:], threshold[:]) >= 0 {
					want++
				}
			}
			got, err := b.eligibleCount(node, threshold)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
	}
	if len(model) == 0 {
		require.Zero(t, pages)
	} else {
		require.Equal(t, 2*len(model)-1, pages)
	}
	requireCountStore(t, s, roots...)
}

func TestSequenceIndexReference(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		s := NewCountStore()
		index := SequenceIndex{1}
		model := make(map[uint64]CountKey)
		for range 60 {
			seq := rapid.Uint64Range(0, 15).Draw(rt, "sequence")
			at := countTestKey(rapid.Uint64Range(0, 31).Draw(rt, "time"))
			add := rapid.Bool().Draw(rt, "insert")
			_, exists := model[seq]
			before := backendSnapshot(s.backend.(*MemoryIndexBackend))
			b := s.Begin()
			var err error
			if add {
				err = b.InsertSequence(index, seq, at)
			} else {
				err = b.DeleteSequence(index, seq)
			}
			if add == exists {
				require.Error(rt, err)
				require.Error(rt, b.Commit())
				require.Equal(rt, before, s.backend.(*MemoryIndexBackend).values)
			} else {
				require.NoError(rt, err)
				require.NoError(rt, b.Commit())
				if add {
					model[seq] = at
				} else {
					delete(model, seq)
				}
				require.LessOrEqual(rt, b.Stats().Reads, 65*4000)
				require.LessOrEqual(rt, b.Stats().Writes, 65*800)
			}
			low := rapid.Uint64Range(0, 15).Draw(rt, "low")
			high := rapid.Uint64Range(low, 15).Draw(rt, "high")
			threshold := countTestKey(rapid.Uint64Range(0, 32).Draw(rt, "threshold"))
			query := s.Begin()
			var count uint64
			var eligible []uint64
			for key, value := range model {
				if bytes.Compare(value[:], threshold[:]) >= 0 {
					eligible = append(eligible, key)
					if low <= key && key <= high {
						count++
					}
				}
			}
			actual, err := query.SequenceCount(index, low, high, threshold)
			require.NoError(rt, err)
			require.Equal(rt, count, actual)
			slices.Sort(eligible)
			for _, last := range []bool{false, true} {
				entry, ok, err := query.SequenceExtreme(index, threshold, last)
				require.NoError(rt, err)
				require.Equal(rt, len(eligible) > 0, ok)
				if ok {
					selected := eligible[0]
					if last {
						selected = eligible[len(eligible)-1]
					}
					require.Equal(rt, SequenceEntry{Sequence: selected, Time: model[selected]}, entry)
				}
			}
			for key, value := range model {
				got, ok, err := query.Sequence(index, key)
				require.NoError(rt, err)
				require.True(rt, ok)
				require.Equal(rt, SequenceEntry{Sequence: key, Time: value}, got)
			}
			query.Abort()
			requireSequenceStore(rt, s, index, model)
		}
	})
}

func TestSequenceIndexRollbackAndGenerations(t *testing.T) {
	t.Parallel()
	backend := &faultIndexBackend{memory: NewMemoryIndexBackend()}
	s := NewCountStoreWithBackend(backend)
	a, b := SequenceIndex{1}, SequenceIndex{2}
	batch := s.Begin()
	for _, index := range []SequenceIndex{a, b} {
		for _, seq := range []uint64{0, 1, math.MaxUint64} {
			require.NoError(t, batch.InsertSequence(index, seq, countTestKey(10)))
		}
	}
	require.NoError(t, batch.Commit())
	require.Equal(t, 1, backend.applies)
	before := backendSnapshot(backend.memory)
	batch = s.Begin()
	require.NoError(t, batch.DeleteSequence(a, 0))
	require.NoError(t, batch.InsertSequence(b, 2, countTestKey(1)))
	backend.failApply = true
	require.ErrorContains(t, batch.Commit(), "publication")
	require.Equal(t, before, backend.memory.values)
	backend.failApply = false
	batch = s.Begin()
	// Collapsing A must release only the duplicate parent summary; B still owns its pages.
	for _, seq := range []uint64{0, 1, math.MaxUint64} {
		require.NoError(t, batch.DeleteSequence(a, seq))
	}
	require.NoError(t, batch.Commit())
	query := s.Begin()
	defer query.Abort()
	count, err := query.SequenceCount(a, 0, math.MaxUint64, CountKey{})
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = query.SequenceCount(b, 0, math.MaxUint64, CountKey{})
	require.NoError(t, err)
	require.EqualValues(t, 3, count)
}

func TestSequenceIndexAdversarialDepthAndStorage(t *testing.T) {
	t.Parallel()
	for _, n := range []int{4, 5, 1000, 10000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			s := NewCountStore()
			index := SequenceIndex{3}
			var peak CountStats
			for i := range n {
				batch := s.Begin()
				// Permuted time order prevents sequence subtrees from trivially sharing time subtrees.
				require.NoError(t, batch.InsertSequence(index, uint64(i), countTestKey(uint64(i)*11400714819323198485)))
				require.NoError(t, batch.Commit())
				stats := batch.Stats()
				peak.Reads = max(peak.Reads, stats.Reads)
				peak.Writes = max(peak.Writes, stats.Writes)
				peak.MutationBytes = max(peak.MutationBytes, stats.MutationBytes)
				peak.KeyBytes = max(peak.KeyBytes, stats.KeyBytes)
				peak.EncodedBytes = max(peak.EncodedBytes, stats.EncodedBytes)
				peak.Releases = max(peak.Releases, stats.Releases)
			}
			memory := s.backend.(*MemoryIndexBackend)
			valueBytes, keyBytes := 0, 0
			for key, value := range memory.values {
				valueBytes += len(value)
				keyBytes += len(key)
			}
			t.Logf("N=%d pages=%d value_bytes=%d key_bytes=%d peak=%+v; excludes CHASM envelopes/heap", n, len(memory.values), valueBytes, keyBytes, peak)
			b := s.Begin()
			count, err := b.SequenceCount(index, 0, uint64(n/2), countTestKey(uint64(n/2)))
			require.NoError(t, err)
			var expected uint64
			for i := 0; i <= n/2; i++ {
				if uint64(i)*11400714819323198485 >= uint64(n/2) {
					expected++
				}
			}
			require.Equal(t, expected, count)
			require.LessOrEqual(t, b.Stats().Reads, 65*4000)
			b.Abort()
		})
	}
	s := NewCountStore()
	index := SequenceIndex{4}
	sequences := []uint64{0}
	for bit := range 64 {
		sequences = append(sequences, uint64(1)<<bit)
	}
	for i, seq := range sequences {
		b := s.Begin()
		require.NoError(t, b.InsertSequence(index, seq, countTestKey(uint64(len(sequences)-i))))
		require.NoError(t, b.Commit())
		require.LessOrEqual(t, b.Stats().Reads, 65*4000)
	}
	b := s.Begin()
	require.NoError(t, b.DeleteSequence(index, 0))
	require.NoError(t, b.Commit())
	t.Logf("full sequence-depth deletion=%+v", b.Stats())
	require.LessOrEqual(t, b.Stats().Reads, 65*4000)
	require.LessOrEqual(t, b.Stats().Writes, 65*800)
}

func TestSequenceIndexEveryReadFailureRollsBack(t *testing.T) {
	t.Parallel()
	base := NewMemoryIndexBackend()
	s := NewCountStoreWithBackend(base)
	index := SequenceIndex{5}
	for _, seq := range []uint64{0, 1, 4, 7, 8, 15} {
		b := s.Begin()
		require.NoError(t, b.InsertSequence(index, seq, countTestKey(30-seq)))
		require.NoError(t, b.Commit())
	}
	snapshot := backendSnapshot(base)
	for _, remove := range []bool{false, true} {
		operation := func(b *CountBatch) error {
			if remove {
				return b.DeleteSequence(index, 7)
			}
			return b.InsertSequence(index, 6, countTestKey(100))
		}
		successful := &faultIndexBackend{memory: &MemoryIndexBackend{values: backendSnapshot(base)}}
		b := NewCountStoreWithBackend(successful).Begin()
		require.NoError(t, operation(b))
		require.NoError(t, b.Commit())
		for fault := 1; fault <= successful.reads; fault++ {
			backend := &faultIndexBackend{memory: &MemoryIndexBackend{values: backendSnapshot(base)}, failRead: fault}
			b := NewCountStoreWithBackend(backend).Begin()
			require.ErrorContains(t, operation(b), "injected read failure", "remove=%v read=%d", remove, fault)
			require.ErrorContains(t, b.Commit(), "injected read failure")
			require.Zero(t, backend.applies)
			require.Equal(t, snapshot, backend.memory.values)
		}
	}
}

func TestSequenceIndexClockRollbackRectangle(t *testing.T) {
	t.Parallel()
	s := NewCountStore()
	index := SequenceIndex{6}
	b := s.Begin()
	for i, at := range []uint64{109, 102, 110} {
		require.NoError(t, b.InsertSequence(index, uint64(i+1), countTestKey(at)))
	}
	require.NoError(t, b.Commit())
	b = s.Begin()
	defer b.Abort()
	count, err := b.SequenceCount(index, 1, 3, countTestKey(107))
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = b.SequenceCount(index, 2, 2, countTestKey(107))
	require.NoError(t, err)
	require.Zero(t, count)
	first, ok, err := b.SequenceExtreme(index, countTestKey(107), false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, SequenceEntry{Sequence: 1, Time: countTestKey(109)}, first)
	last, ok, err := b.SequenceExtreme(index, countTestKey(107), true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, SequenceEntry{Sequence: 3, Time: countTestKey(110)}, last)
}

func TestSequenceInlineLeafCollapseSharedCountOwner(t *testing.T) {
	t.Parallel()
	for _, sameTime := range []bool{false, true} {
		for _, removed := range []uint64{0, 1} {
			t.Run(fmt.Sprintf("same_time_%v_remove_%d", sameTime, removed), func(t *testing.T) {
				t.Parallel()
				s := NewCountStore()
				index := SequenceIndex{9}
				times := [2]CountKey{countTestKey(7), countTestKey(8)}
				if sameTime {
					times[1] = times[0]
				}
				survivor := uint64(1) - removed
				owner, _, err := s.Update(CountRoot{}, times[survivor], true)
				require.NoError(t, err)
				b := s.Begin()
				require.NoError(t, b.InsertSequence(index, 0, times[0]))
				require.NoError(t, b.InsertSequence(index, 1, times[1]))
				require.NoError(t, b.Commit())
				b = s.Begin()
				require.NoError(t, b.DeleteSequence(index, removed))
				require.NoError(t, b.Commit())
				count, err := s.Count(owner)
				require.NoError(t, err)
				require.EqualValues(t, 1, count)
				// The only secondary owner is the independent handle. The surviving outer
				// leaf owns its inline time and cannot release or fabricate a count page.
				requireCountStore(t, s, owner)
				b = s.Begin()
				entry, ok, err := b.Sequence(index, survivor)
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, SequenceEntry{Sequence: survivor, Time: times[survivor]}, entry)
				b.Abort()
				b = s.Begin()
				require.NoError(t, b.DeleteSequence(index, survivor))
				require.NoError(t, b.Commit())
				requireCountStore(t, s, owner)
			})
		}
	}
}
