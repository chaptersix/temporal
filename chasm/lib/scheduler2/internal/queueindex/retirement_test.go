package queueindex

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func retirementTestKey(n int) RetirementKey {
	key, err := NewRetirementKey(int64(n/32)-2, fmt.Sprintf("manual/%03d/action/start", n))
	if err != nil {
		panic(err)
	}
	return key
}

func requireRetirementStore(t require.TestingT, store *CountStore, index RetirementIndex, keys []RetirementKey) {
	b := store.Begin()
	defer b.Abort()
	root, err := b.retirementRoot(index)
	require.NoError(t, err)
	seen := make(map[string]bool)
	actual := make([]RetirementKey, 0)
	var walk func(retirementID)
	walk = func(id retirementID) {
		name := index.pageKey(id)
		require.False(t, seen[name])
		seen[name] = true
		n, e := b.retirementNode(index, id)
		require.NoError(t, e)
		if len(n.keys) > 0 {
			actual = append(actual, n.keys...)
			return
		}
		_, _, e = b.retirementChildren(index, n)
		require.NoError(t, e)
		walk(n.left)
		walk(n.right)
	}
	if root != (retirementID{}) {
		walk(root)
		seen[index.rootKey()] = true
	}
	expected := slices.Clone(keys)
	slices.SortFunc(expected, retirementCompare)
	require.Equal(t, expected, actual)
	for name := range store.backend.(*MemoryIndexBackend).values {
		if strings.HasPrefix(name, "retirement") {
			require.True(t, seen[name], name)
		}
	}
}

func TestRetirementReference(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		store := NewCountStore()
		index := RetirementIndex{1}
		model := make(map[int]bool)
		for range 100 {
			value := rapid.IntRange(0, 95).Draw(rt, "key")
			b := store.Begin()
			if model[value] {
				require.NoError(rt, b.DeleteRetirement(index, retirementTestKey(value)))
				delete(model, value)
			} else {
				require.NoError(rt, b.InsertRetirement(index, retirementTestKey(value)))
				model[value] = true
			}
			keys := make([]RetirementKey, 0, len(model))
			for n := range model {
				keys = append(keys, retirementTestKey(n))
			}
			slices.SortFunc(keys, retirementCompare)
			key, ok, err := b.RetirementMinimum(index)
			require.NoError(rt, err)
			count, err := b.RetirementCount(index)
			require.NoError(rt, err)
			require.Equal(rt, uint64(len(keys)), count)
			require.Equal(rt, len(keys) > 0, ok)
			if ok {
				require.Equal(rt, keys[0], key)
			}
			require.NoError(rt, b.Commit())
			requireRetirementStore(rt, store, index, keys)
		}
	})
}

func TestRetirementCanonicalOrderAndCollapse(t *testing.T) {
	t.Parallel()
	index := RetirementIndex{2}
	stores := []*CountStore{NewCountStore(), NewCountStore()}
	keys := make([]RetirementKey, 97)
	for i := range keys {
		keys[i] = retirementTestKey(i)
	}
	for which, store := range stores {
		for i := range keys {
			at := i
			if which == 1 {
				at = len(keys) - 1 - i
			}
			b := store.Begin()
			require.NoError(t, b.InsertRetirement(index, keys[at]))
			require.NoError(t, b.Commit())
		}
	}
	require.Equal(t, backendSnapshot(stores[0].backend.(*MemoryIndexBackend)), backendSnapshot(stores[1].backend.(*MemoryIndexBackend)))
	for i := range 81 {
		b := stores[0].Begin()
		require.NoError(t, b.DeleteRetirement(index, keys[i]))
		require.NoError(t, b.Commit())
		requireRetirementStore(t, stores[0], index, keys[i+1:])
	}
	rebuilt := NewCountStore()
	b := rebuilt.Begin()
	for _, key := range keys[81:] {
		require.NoError(t, b.InsertRetirement(index, key))
	}
	require.NoError(t, b.Commit())
	require.Equal(t, backendSnapshot(rebuilt.backend.(*MemoryIndexBackend)), backendSnapshot(stores[0].backend.(*MemoryIndexBackend)))
}

func TestRetirementKeyOrdering(t *testing.T) {
	t.Parallel()
	suffixes := []string{"a", "a/action/start", "aa", "backfill/18446744073709551615/9999-12-31T23:59:59.999999999Z/action/start", "z"}
	var keys []RetirementKey
	for _, nominal := range []int64{math.MinInt64, -1, 0, math.MaxInt64} {
		for _, suffix := range suffixes {
			key, err := NewRetirementKey(nominal, suffix)
			require.NoError(t, err)
			require.Equal(t, nominal, key.Nominal())
			require.Equal(t, suffix, key.Suffix())
			keys = append(keys, key)
		}
	}
	require.True(t, slices.IsSortedFunc(keys, retirementCompare))
	for _, suffix := range []string{"", strings.Repeat("x", 74), "a\x00b", "é"} {
		_, err := NewRetirementKey(0, suffix)
		require.Error(t, err)
	}
	store := NewCountStore()
	b := store.Begin()
	for _, key := range keys {
		require.NoError(t, b.InsertRetirement(RetirementIndex{}, key))
	}
	require.NoError(t, b.Commit())
	requireRetirementStore(t, store, RetirementIndex{}, keys)
}

func TestRetirementFailureAtomicity(t *testing.T) {
	t.Parallel()
	index := RetirementIndex{3}
	base := NewMemoryIndexBackend()
	store := NewCountStoreWithBackend(base)
	b := store.Begin()
	for i := range 40 {
		require.NoError(t, b.InsertRetirement(index, retirementTestKey(i)))
	}
	require.NoError(t, b.Commit())
	snapshot := backendSnapshot(base)
	actions := map[string]func(*CountBatch) error{
		"count":   func(b *CountBatch) error { _, err := b.RetirementCount(index); return err },
		"minimum": func(b *CountBatch) error { _, _, err := b.RetirementMinimum(index); return err },
		"insert":  func(b *CountBatch) error { return b.InsertRetirement(index, retirementTestKey(96)) },
		"delete":  func(b *CountBatch) error { return b.DeleteRetirement(index, retirementTestKey(0)) },
	}
	for name, action := range actions {
		t.Run(name, func(t *testing.T) {
			probe := &faultIndexBackend{memory: base}
			batch := NewCountStoreWithBackend(probe).Begin()
			require.NoError(t, action(batch))
			batch.Abort()
			for read := 1; read <= probe.reads; read++ {
				fault := &faultIndexBackend{memory: base, failRead: read}
				batch = NewCountStoreWithBackend(fault).Begin()
				require.Error(t, action(batch))
				require.Error(t, batch.Commit())
				require.Zero(t, fault.applies)
				require.Equal(t, snapshot, base.values)
			}
			fault := &faultIndexBackend{memory: base, failApply: true}
			batch = NewCountStoreWithBackend(fault).Begin()
			require.NoError(t, action(batch))
			require.Error(t, batch.Commit())
			require.Equal(t, snapshot, base.values)
		})
	}
	// The ready and retirement changes share exactly one publication boundary.
	fault := &faultIndexBackend{memory: base, failApply: true}
	b = NewCountStoreWithBackend(fault).Begin()
	require.NoError(t, b.DeleteRetirement(index, retirementTestKey(0)))
	require.NoError(t, b.InsertSequence(SequenceIndex{3}, 1, countTestKey(2)))
	require.Error(t, b.Commit())
	require.Equal(t, snapshot, base.values)
}

func TestRetirementCorruption(t *testing.T) {
	t.Parallel()
	index := RetirementIndex{4}
	for _, kind := range []string{"root", "missing", "count", "prefix", "bucket"} {
		t.Run(kind, func(t *testing.T) {
			store := NewCountStore()
			b := store.Begin()
			for i := range 40 {
				require.NoError(t, b.InsertRetirement(index, retirementTestKey(i)))
			}
			require.NoError(t, b.Commit())
			b = store.Begin()
			id, err := b.retirementRoot(index)
			require.NoError(t, err)
			n, err := b.retirementNode(index, id)
			require.NoError(t, err)
			memory := store.backend.(*MemoryIndexBackend)
			switch kind {
			case "root":
				memory.values[index.rootKey()] = []byte{0}
			case "missing":
				delete(memory.values, index.pageKey(n.left))
			case "count":
				n.count++
				memory.values[index.pageKey(id)] = n.encode()
			case "prefix":
				n.left = n.right
				memory.values[index.pageKey(id)] = n.encode()
			case "bucket":
				for len(n.keys) == 0 {
					n, err = b.retirementNode(index, n.left)
					require.NoError(t, err)
				}
				n.keys[0][87] = 1
				memory.values[index.pageKey(n.id)] = n.encode()
			default:
				t.Fatal("unknown corruption")
			}
			b.Abort()
			snapshot := backendSnapshot(memory)
			b = store.Begin()
			_, _, err = b.RetirementMinimum(index)
			require.Error(t, err)
			require.Error(t, b.Commit())
			require.Equal(t, snapshot, memory.values)
		})
	}
}

func TestRetirementDepthAndStorage(t *testing.T) {
	t.Parallel()
	for _, size := range []int{4, 5, 1000, 10000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			store := NewCountStore()
			index := RetirementIndex{5}
			b := store.Begin()
			for i := range size {
				key, err := NewRetirementKey(int64(uint64(i)*0x9e3779b97f4a7c15), fmt.Sprintf("backfill/%020d/9999-12-31T23:59:59.999999999Z/action/start", i))
				require.NoError(t, err)
				require.NoError(t, b.InsertRetirement(index, key))
			}
			require.NoError(t, b.Commit())
			memory := store.backend.(*MemoryIndexBackend)
			payload, path := 0, 0
			for key, value := range memory.values {
				path += len(key)
				payload += len(value)
			}
			b = store.Begin()
			_, ok, err := b.RetirementMinimum(index)
			require.NoError(t, err)
			require.True(t, ok)
			require.LessOrEqual(t, b.Stats().Reads, 2820)
			require.Empty(t, b.extra)
			t.Logf("N=%d pages=%d payload=%d keys=%d total=%d minimum_reads=%d", size, len(memory.values), payload, path, payload+path, b.Stats().Reads)
			b.Abort()
		})
	}
	// Flip each usable bit independently, forcing long chains of one-sided
	// partitions. ASCII and the generated suffix bound exclude some of 704 bits.
	key, err := NewRetirementKey(math.MinInt64, strings.Repeat("\x01", 73))
	require.NoError(t, err)
	keys := []RetirementKey{key}
	for i := range 81 {
		for bit := range 8 {
			other := key
			other[i] ^= 1 << uint(bit)
			if retirementValidKey(other) {
				keys = append(keys, other)
			}
		}
	}
	store := NewCountStore()
	index := RetirementIndex{6}
	b := store.Begin()
	for _, key := range keys {
		require.NoError(t, b.InsertRetirement(index, key))
	}
	require.NoError(t, b.Commit())
	requireRetirementStore(t, store, index, keys)
	b = store.Begin()
	minimum, ok, err := b.RetirementMinimum(index)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, slices.MinFunc(keys, retirementCompare), minimum)
	require.LessOrEqual(t, b.Stats().Reads, 2820)
	t.Logf("depth witness keys=%d minimum_reads=%d", len(keys), b.Stats().Reads)
	b.Abort()
	for _, key := range keys {
		b = store.Begin()
		require.NoError(t, b.DeleteRetirement(index, key))
		require.NoError(t, b.Commit())
		require.LessOrEqual(t, b.Stats().Writes, 2116)
	}
	require.Empty(t, store.backend.(*MemoryIndexBackend).values)
}

func TestRetirementPresenceAndOwnerIsolation(t *testing.T) {
	t.Parallel()
	store := NewCountStore()
	first, second := RetirementIndex{7}, RetirementIndex{8}
	key := retirementTestKey(0)
	b := store.Begin()
	require.NoError(t, b.InsertRetirement(first, key))
	require.NoError(t, b.InsertRetirement(second, key))
	require.NoError(t, b.Commit())
	snapshot := backendSnapshot(store.backend.(*MemoryIndexBackend))
	b = store.Begin()
	require.Error(t, b.InsertRetirement(first, key))
	require.Error(t, b.Commit())
	require.Equal(t, snapshot, store.backend.(*MemoryIndexBackend).values)
	b = store.Begin()
	require.Error(t, b.DeleteRetirement(first, retirementTestKey(1)))
	require.Error(t, b.Commit())
	require.Equal(t, snapshot, store.backend.(*MemoryIndexBackend).values)
	b = store.Begin()
	require.NoError(t, b.DeleteRetirement(first, key))
	require.NoError(t, b.Commit())
	b = store.Begin()
	actual, ok, err := b.RetirementMinimum(second)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, actual)
	b.Abort()
}

func TestRetirementSplitCollapseReadFailures(t *testing.T) {
	t.Parallel()
	for _, size := range []int{16, 17} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			memory := NewMemoryIndexBackend()
			store := NewCountStoreWithBackend(memory)
			index := RetirementIndex{9}
			b := store.Begin()
			for i := range size {
				require.NoError(t, b.InsertRetirement(index, retirementTestKey(i)))
			}
			require.NoError(t, b.Commit())
			snapshot := backendSnapshot(memory)
			change := func(b *CountBatch) error {
				if size == 16 {
					return b.InsertRetirement(index, retirementTestKey(16))
				}
				return b.DeleteRetirement(index, retirementTestKey(0))
			}
			probe := &faultIndexBackend{memory: memory}
			b = NewCountStoreWithBackend(probe).Begin()
			require.NoError(t, change(b))
			b.Abort()
			for read := 1; read <= probe.reads; read++ {
				fault := &faultIndexBackend{memory: memory, failRead: read}
				b = NewCountStoreWithBackend(fault).Begin()
				require.Error(t, change(b))
				require.Error(t, b.Commit())
				require.Zero(t, fault.applies)
				require.Equal(t, snapshot, memory.values)
			}
			fault := &faultIndexBackend{memory: memory, failApply: true}
			b = NewCountStoreWithBackend(fault).Begin()
			require.NoError(t, change(b))
			require.Error(t, b.Commit())
			require.Equal(t, snapshot, memory.values)
		})
	}
}
