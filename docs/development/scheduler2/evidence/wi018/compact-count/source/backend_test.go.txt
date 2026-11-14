package queueindex

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type faultIndexBackend struct {
	memory                   *MemoryIndexBackend
	reads, applies, failRead int
	failApply                bool
	last                     []IndexChange
}

func (b *faultIndexBackend) Read(key string) ([]byte, bool, error) {
	b.reads++
	if b.failRead == b.reads {
		return nil, false, errors.New("injected read failure")
	}
	return b.memory.Read(key)
}
func (b *faultIndexBackend) Apply(changes []IndexChange) error {
	b.applies++
	if b.failApply {
		return errors.New("injected publication failure")
	}
	b.last = changes
	return b.memory.Apply(changes)
}
func backendSnapshot(b *MemoryIndexBackend) map[string][]byte {
	snapshot := make(map[string][]byte, len(b.values))
	for key, value := range b.values {
		snapshot[key] = bytes.Clone(value)
	}
	return snapshot
}

func TestCountBatchAtomicAncestors(t *testing.T) {
	t.Parallel()
	backend := &faultIndexBackend{memory: NewMemoryIndexBackend()}
	s := NewCountStoreWithBackend(backend)
	batch := s.Begin()
	root, err := batch.Update(CountRoot{}, countTestKey(10), true)
	require.NoError(t, err)
	require.NoError(t, batch.Fork(root))
	child, err := batch.Update(root, countTestKey(20), true)
	require.NoError(t, err)
	require.NoError(t, batch.stageIndex("sequence/parent", child[:], false))
	require.NoError(t, batch.stageIndex("root/queue", root[:], false))
	count, err := batch.Count(child)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	require.Empty(t, backend.memory.values, "nothing publishes during ancestor preparation")
	require.NoError(t, batch.Commit())
	require.Equal(t, 1, backend.applies)
	require.Equal(t, child[:], backend.memory.values["sequence/parent"])
	require.Equal(t, root[:], backend.memory.values["root/queue"])
	var mutationBytes, keyBytes int
	for _, change := range backend.last {
		mutationBytes += len(change.Value)
		keyBytes += len(change.Key)
	}
	require.Equal(t, mutationBytes, batch.Stats().MutationBytes)
	require.Equal(t, keyBytes, batch.Stats().KeyBytes)
	require.ErrorContains(t, batch.Commit(), "closed")
	requireCountStore(t, NewCountStoreWithBackend(backend.memory), root, child)
}

func TestCountBatchFailurePoisonsAllLayers(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"later_read", "publication", "missing_key"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			backend := &faultIndexBackend{memory: NewMemoryIndexBackend()}
			s := NewCountStoreWithBackend(backend)
			root, _, err := s.Update(CountRoot{}, countTestKey(1), true)
			require.NoError(t, err)
			before := backendSnapshot(backend.memory)
			batch := s.Begin()
			next, err := batch.Update(root, countTestKey(2), true)
			require.NoError(t, err)
			require.NoError(t, batch.stageIndex("sequence/parent", next[:], false))
			require.NoError(t, batch.stageIndex("root/queue", next[:], false))
			var want string
			switch kind {
			case "later_read":
				backend.failRead = backend.reads + 1
				_, err = batch.Update(next, countTestKey(3), true)
				want = "injected read failure"
			case "publication":
				backend.failApply = true
				err = batch.Commit()
				want = "injected publication failure"
			case "missing_key":
				_, err = batch.Update(next, countTestKey(3), false)
				want = "count key absent"
			default:
				t.Fatal("unexpected fault")
			}
			require.ErrorContains(t, err, want)
			reads, applies := backend.reads, backend.applies
			require.ErrorContains(t, batch.Fork(next), want)
			_, _, err = batch.CountBefore(next, countTestKey(100))
			require.ErrorContains(t, err, want)
			require.ErrorContains(t, batch.Commit(), want)
			require.Equal(t, reads, backend.reads)
			require.Equal(t, applies, backend.applies)
			require.Equal(t, before, backend.memory.values)
		})
	}
}

func TestCountBatchCanonicalRecreationAndAbort(t *testing.T) {
	t.Parallel()
	s := NewCountStore()
	root, _, err := s.Update(CountRoot{}, countTestKey(1), true)
	require.NoError(t, err)
	backend := s.backend.(*MemoryIndexBackend)
	before := backendSnapshot(backend)
	batch := s.Begin()
	empty, err := batch.Update(root, countTestKey(1), false)
	require.NoError(t, err)
	require.Equal(t, CountRoot{}, empty)
	restored, err := batch.Update(empty, countTestKey(1), true)
	require.NoError(t, err)
	require.Equal(t, root, restored, "staged deletion must not revive stale backend refs")
	require.NoError(t, batch.Commit())
	require.Equal(t, before, backend.values)
	batch = s.Begin()
	_, err = batch.Update(root, countTestKey(2), true)
	require.NoError(t, err)
	batch.Abort()
	require.ErrorContains(t, batch.Commit(), "closed")
	require.Equal(t, before, backend.values)
}

func TestCountBatchReferenceOverflowAfterAncestor(t *testing.T) {
	t.Parallel()
	s := NewCountStore()
	a, _, err := s.Update(CountRoot{}, countTestKey(1), true)
	require.NoError(t, err)
	var b CountRoot
	for key := uint64(100); key < 117; key++ {
		b, _, err = s.Update(b, countTestKey(key), true)
		require.NoError(t, err)
	}
	entries := countTestEntries(t, s)
	shared := entries[b].Node.Right
	entry := entries[shared]
	entry.Refs = ^uint64(0)
	require.NoError(t, s.backend.Apply([]IndexChange{{Key: countPageKey(shared), Value: entry.encode()}}))
	backend := s.backend.(*MemoryIndexBackend)
	before := backendSnapshot(backend)
	batch := s.Begin()
	nextA, err := batch.Update(a, countTestKey(2), true)
	require.NoError(t, err)
	require.NoError(t, batch.stageIndex("root/ancestor", nextA[:], false))
	_, err = batch.Update(b, countTestKey(101), true)
	require.ErrorContains(t, err, "references exhausted")
	require.ErrorContains(t, batch.Commit(), "references exhausted")
	require.Equal(t, before, backend.values)
}

func TestCountBatchRejectsMalformedTouchedBranches(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"nonincreasing_bit", "wrong_aggregate", "missing_child"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			s := NewCountStore()
			put := func(node countNode) CountRoot {
				wire := node.encode()
				id := CountRoot(sha256.Sum256(wire[:]))
				require.NoError(t, s.backend.Apply([]IndexChange{{Key: countPageKey(id), Value: (countEntry{Node: node, Refs: 1}).encode()}}))
				return id
			}
			zero := put(countLeaf(CountKey{}, 1))
			high := put(countLeaf(CountKey{0x80}, 1))
			child := put(countNode{Bit: 0, Count: 20, Distinct: 20, Left: zero, Right: high})
			right := put(countLeaf(CountKey{0x40}, 1))
			malformed := countNode{Bit: 1, Count: 21, Distinct: 21, Left: child, Right: right}
			if kind != "nonincreasing_bit" {
				malformed = countNode{Bit: 0, Count: 20, Distinct: 20, Left: zero, Right: high}
			}
			if kind == "wrong_aggregate" {
				malformed.Count = 21
			}
			root := put(malformed)
			if kind == "missing_child" {
				require.NoError(t, s.backend.Apply([]IndexChange{{Key: countPageKey(high), Delete: true}}))
			}
			good, _, err := s.Update(CountRoot{}, countTestKey(10), true)
			require.NoError(t, err)
			backend := s.backend.(*MemoryIndexBackend)
			before := backendSnapshot(backend)
			batch := s.Begin()
			goodNext, err := batch.Update(good, countTestKey(11), true)
			require.NoError(t, err)
			require.NoError(t, batch.stageIndex("root/good", goodNext[:], false))
			_, err = batch.Update(root, CountKey{}, true)
			require.Error(t, err)
			require.Error(t, batch.Commit())
			require.Equal(t, before, backend.values)
			_, err = s.Count(root)
			require.Error(t, err)
			_, reads, err := s.CountBefore(root, CountKey{0xff})
			require.Error(t, err)
			require.LessOrEqual(t, reads, 3)
		})
	}
}
