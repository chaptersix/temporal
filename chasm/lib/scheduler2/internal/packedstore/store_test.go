package packedstore

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

type memoryBackend struct {
	data            map[string][]byte
	reads, failRead int
	failApply       bool
	maxChanges      int
}

func (m *memoryBackend) Read(k string) ([]byte, bool, error) {
	m.reads++
	if m.failRead == m.reads {
		return nil, false, errors.New("injected read fault")
	}
	v, ok := m.data[k]
	return bytes.Clone(v), ok, nil
}
func (m *memoryBackend) Apply(changes []queueindex.IndexChange) error {
	if m.failApply {
		return errors.New("injected atomic apply fault")
	}
	m.maxChanges = max(m.maxChanges, len(changes))
	for _, c := range changes {
		if c.Delete {
			delete(m.data, c.Key)
		} else {
			m.data[c.Key] = bytes.Clone(c.Value)
		}
	}
	return nil
}
func inspect(t *testing.T, m *memoryBackend, id ID, reference map[string][]byte) {
	t.Helper()
	b := NewBatch(m)
	n, err := b.Count(id)
	require.NoError(t, err)
	require.EqualValues(t, len(reference), n)
	c, err := b.loadControl(id)
	require.NoError(t, err)
	seen := map[uint64]bool{}
	var walk func(uint64, bool)
	walk = func(pid uint64, root bool) {
		require.False(t, seen[pid])
		seen[pid] = true
		p, err := b.load(id, pid)
		require.NoError(t, err)
		require.LessOrEqual(t, len(p.entries), 16)
		if !root {
			require.GreaterOrEqual(t, len(p.entries), 8)
		}
		if p.leaf {
			for _, e := range p.entries {
				require.Equal(t, reference[string(e.key)], e.value)
			}
		} else {
			for _, e := range p.entries {
				_, err := b.child(id, e)
				require.NoError(t, err)
				walk(e.child, false)
			}
		}
	}
	if c.root != 0 {
		walk(c.root, true)
	}
	controls := 0
	if _, ok := m.data[key(id, 0)]; ok {
		controls = 1
	}
	require.Len(t, m.data, len(seen)+controls)
	for k, want := range reference {
		got, ok, err := b.Get(id, []byte(k))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, want, got)
	}
}
func TestPackedStoreMutableReference(t *testing.T) {
	m := &memoryBackend{data: map[string][]byte{}}
	id := ID{1}
	reference := map[string][]byte{}
	rng := rand.New(rand.NewSource(17))
	for i := range 1600 {
		k := fmt.Sprintf("exact-key/%04d", rng.Intn(300))
		b := NewBatch(m)
		if i%4 == 0 {
			require.NoError(t, b.Delete(id, []byte(k)))
			delete(reference, k)
		} else {
			value := []byte(fmt.Sprintf("value/%d", i))
			require.NoError(t, b.Put(id, []byte(k), value))
			reference[k] = value
		}
		require.NoError(t, b.Commit())
		inspect(t, m, id, reference)
	}
	for k := range maps.Clone(reference) {
		b := NewBatch(m)
		require.NoError(t, b.Delete(id, []byte(k)))
		delete(reference, k)
		require.NoError(t, b.Commit())
		inspect(t, m, id, reference)
	}
}
func TestPackedStoreAtomicFaults(t *testing.T) {
	source := &memoryBackend{data: map[string][]byte{}}
	id := ID{2}
	b := NewBatch(source)
	for i := range 200 {
		require.NoError(t, b.Put(id, []byte(fmt.Sprintf("%04d", i)), []byte("value")))
	}
	require.NoError(t, b.Commit())
	probe := &memoryBackend{data: maps.Clone(source.data)}
	b = NewBatch(probe)
	require.NoError(t, b.Delete(id, []byte("0000")))
	reads := probe.reads
	for fault := 1; fault <= reads; fault++ {
		m := &memoryBackend{data: maps.Clone(source.data), failRead: fault}
		b = NewBatch(m)
		require.Error(t, b.Delete(id, []byte("0000")))
		require.Error(t, b.Commit())
		require.Equal(t, source.data, m.data)
	}
	m := &memoryBackend{data: maps.Clone(source.data), failApply: true}
	b = NewBatch(m)
	require.NoError(t, b.Put(id, []byte("new"), []byte("a")))
	require.NoError(t, b.Put(ID{3}, []byte("other namespace"), []byte("b")))
	require.Error(t, b.Commit())
	require.Equal(t, source.data, m.data)
}
func TestPackedStoreMalformedPages(t *testing.T) {
	for _, wire := range [][]byte{nil, {1, 1, 17}, {1, 0, 1, 0, 0, 0, 0}, {1, 1, 1, 0, 0, 9}} {
		_, err := decode(wire)
		require.Error(t, err)
	}
	m := &memoryBackend{data: map[string][]byte{}}
	b := NewBatch(m)
	id := ID{4}
	require.NoError(t, b.Put(id, []byte("a"), []byte("b")))
	require.NoError(t, b.Commit())
	b = NewBatch(m)
	c, err := b.loadControl(id)
	require.NoError(t, err)
	c.count++
	b.saveControl(id, c)
	require.NoError(t, b.Commit())
	b = NewBatch(m)
	_, _, err = b.Get(id, []byte("a"))
	require.Error(t, err)
	require.Error(t, b.Commit())
}

func TestPackedStoreInsertFaultsAndExhaustion(t *testing.T) {
	source := &memoryBackend{data: map[string][]byte{}}
	id := ID{5}
	b := NewBatch(source)
	for i := range 256 {
		require.NoError(t, b.Put(id, []byte(fmt.Sprintf("%04d", i)), []byte("value")))
	}
	require.NoError(t, b.Commit())
	probe := &memoryBackend{data: maps.Clone(source.data)}
	b = NewBatch(probe)
	require.NoError(t, b.Put(id, []byte("0256"), []byte("next")))
	for fault := 1; fault <= probe.reads; fault++ {
		m := &memoryBackend{data: maps.Clone(source.data), failRead: fault}
		b = NewBatch(m)
		require.Error(t, b.Put(id, []byte("0256"), []byte("next")))
		require.Error(t, b.Commit())
		require.Equal(t, source.data, m.data)
	}
	m := &memoryBackend{data: map[string][]byte{}}
	b = NewBatch(m)
	for i := range 16 {
		require.NoError(t, b.Put(id, []byte(fmt.Sprintf("%04d", i)), nil))
	}
	require.NoError(t, b.Commit())
	b = NewBatch(m)
	c, err := b.loadControl(id)
	require.NoError(t, err)
	c.next = ^uint64(0)
	b.saveControl(id, c)
	require.NoError(t, b.Commit())
	before := maps.Clone(m.data)
	b = NewBatch(m)
	require.Error(t, b.Put(id, []byte("0016"), nil))
	require.Error(t, b.Commit())
	require.Equal(t, before, m.data)
}
