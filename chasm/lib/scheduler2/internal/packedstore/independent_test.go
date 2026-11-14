package packedstore

import (
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndependentPackedTouchedChildRange(t *testing.T) {
	m := &memoryBackend{data: map[string][]byte{}}
	id := ID{91}
	b := NewBatch(m)
	left, right := page{leaf: true}, page{leaf: true}
	for i := range 8 {
		left.entries = append(left.entries, entry{key: []byte(fmt.Sprintf("a%d", i))})
		right.entries = append(right.entries, entry{key: []byte(fmt.Sprintf("m%d", i))})
	}
	left.entries[7].key = []byte("z")
	b.saveControl(id, control{root: 1, next: 4, count: 16})
	b.put(key(id, 1), encode(page{entries: []entry{{key: []byte("a0"), child: 2, count: 8}, {key: []byte("m0"), child: 3, count: 8}}}))
	b.put(key(id, 2), encode(left))
	b.put(key(id, 3), encode(right))
	require.NoError(t, b.Commit())
	b = NewBatch(m)
	_, _, err := b.Get(id, []byte("a0"))
	require.Error(t, err, "touched child maximum crosses next parent separator")
	require.Error(t, b.Commit())
}

func TestIndependentPackedAllocationCollision(t *testing.T) {
	m := &memoryBackend{data: map[string][]byte{}}
	id := ID{92}
	b := NewBatch(m)
	for i := range 16 {
		require.NoError(t, b.Put(id, []byte(fmt.Sprintf("%02d", i)), nil))
	}
	require.NoError(t, b.Commit())
	// An existing page at next must not be silently overwritten after malformed
	// allocator control or an incomplete store restore.
	m.data[key(id, 2)] = encode(page{leaf: true, entries: []entry{{key: []byte("owned"), value: []byte("preserve")}}})
	before := maps.Clone(m.data)
	b = NewBatch(m)
	require.Error(t, b.Put(id, []byte("16"), nil))
	require.Error(t, b.Commit())
	require.Equal(t, before, m.data)
}

func TestIndependentPackedChildOutsideAllocationFrontier(t *testing.T) {
	m := &memoryBackend{data: map[string][]byte{}}
	id := ID{93}
	b := NewBatch(m)
	left, right := page{leaf: true}, page{leaf: true}
	for i := range 8 {
		left.entries = append(left.entries, entry{key: []byte(fmt.Sprintf("a%d", i))})
		right.entries = append(right.entries, entry{key: []byte(fmt.Sprintf("m%d", i))})
	}
	b.saveControl(id, control{root: 1, next: 3, count: 16})
	b.put(key(id, 1), encode(page{entries: []entry{{key: []byte("a0"), child: 9, count: 8}, {key: []byte("m0"), child: 2, count: 8}}}))
	b.put(key(id, 9), encode(left))
	b.put(key(id, 2), encode(right))
	require.NoError(t, b.Commit())
	b = NewBatch(m)
	_, _, err := b.Get(id, []byte("a0"))
	require.Error(t, err, "referenced page was never allocated under this control")
	require.Error(t, b.Commit())
}

func TestIndependentPackedIDsAndPoisonedOverlay(t *testing.T) {
	m := &memoryBackend{data: map[string][]byte{}}
	id := ID{94}
	var previous uint64
	for range 12 {
		b := NewBatch(m)
		require.NoError(t, b.Put(id, []byte("key"), []byte("value")))
		c, err := b.loadControl(id)
		require.NoError(t, err)
		require.Greater(t, c.root, previous)
		previous = c.root
		value, ok, err := b.Get(id, []byte("key"))
		require.NoError(t, err)
		require.True(t, ok)
		value[0] = 'x'
		value, ok, err = b.Get(id, []byte("key"))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []byte("value"), value)
		require.NoError(t, b.Commit())
		b = NewBatch(m)
		require.NoError(t, b.Delete(id, []byte("key")))
		require.NoError(t, b.Commit())
	}
	before := maps.Clone(m.data)
	b := NewBatch(m)
	require.NoError(t, b.Put(id, []byte("staged"), []byte("discard")))
	m.failRead = m.reads + 1
	_, _, err := b.Get(ID{95}, []byte("fault"))
	require.Error(t, err)
	_, _, err = b.Get(id, []byte("staged"))
	require.Error(t, err)
	require.Error(t, b.Commit())
	require.Equal(t, before, m.data)
}
