package queueindex

import (
	"bytes"
	"encoding/hex"
	"errors"
)

// IndexChange is one detached keyed mutation. Delete ignores Value.
type IndexChange struct {
	Key    string
	Value  []byte
	Delete bool
}

// IndexBackend stages a complete index mutation atomically relative to Read.
// Apply does not imply a durable database commit: a CHASM adapter must include
// every page and owning root in one enclosing engine transaction.
// Callers serialize batches; this interface provides no cross-writer isolation.
type IndexBackend interface {
	Read(key string) ([]byte, bool, error)
	Apply(changes []IndexChange) error
}

// MemoryIndexBackend is the standalone prototype's keyed storage adapter.
// It is not safe for concurrent use and intentionally exposes no enumeration API.
type MemoryIndexBackend struct{ values map[string][]byte }

// NewMemoryIndexBackend creates empty in-memory storage.
func NewMemoryIndexBackend() *MemoryIndexBackend {
	return &MemoryIndexBackend{values: make(map[string][]byte)}
}

func (b *MemoryIndexBackend) Read(key string) ([]byte, bool, error) {
	value, ok := b.values[key]
	return bytes.Clone(value), ok, nil
}

func (b *MemoryIndexBackend) Apply(changes []IndexChange) error {
	for _, change := range changes {
		if change.Delete {
			delete(b.values, change.Key)
		} else {
			b.values[change.Key] = bytes.Clone(change.Value)
		}
	}
	return nil
}

func countPageKey(id CountRoot) string { return "count/" + hex.EncodeToString(id[:]) }

// CountBatch shares staged secondary-node changes across all ancestor updates.
// Any operation error poisons the batch. Commit publishes once; Abort never writes.
type CountBatch struct {
	tx     *countTxn
	extra  map[string]IndexChange
	err    error
	closed bool
}

// Begin starts an isolated overlay. The caller holds its enclosing execution lock
// until Commit or Abort and must not interleave writes through another batch.
func (s *CountStore) Begin() *CountBatch {
	return &CountBatch{tx: &countTxn{store: s, changed: make(map[CountRoot]*countEntry)}, extra: make(map[string]IndexChange)}
}

func (b *CountBatch) ready() error {
	if b.err != nil {
		return b.err
	}
	if b.closed {
		return errors.New("index batch closed")
	}
	return nil
}

func (b *CountBatch) fail(err error) error {
	if err != nil && b.err == nil {
		b.err = err
	}
	return b.err
}

// Update replaces one owned root inside the batch, without backend publication.
func (b *CountBatch) Update(root CountRoot, key CountKey, add bool) (CountRoot, error) {
	if err := b.ready(); err != nil {
		return root, err
	}
	next, err := b.tx.change(root, key, add)
	if err == nil {
		err = b.tx.retain(next)
	}
	if err == nil {
		err = b.tx.release(root)
	}
	if err != nil {
		return root, b.fail(err)
	}
	return next, nil
}

// Fork stages an additional external owner of root.
func (b *CountBatch) Fork(root CountRoot) error {
	if err := b.ready(); err != nil {
		return err
	}
	return b.fail(b.tx.retain(root))
}

// Stats reports all work so far; mutation byte totals are populated by Commit.
func (b *CountBatch) Stats() CountStats { return b.tx.stats }

// Abort discards all staged changes. It does not release committed root owners.
func (b *CountBatch) Abort() { b.closed = true }

// Commit applies count, outer-index and owning-root changes in one backend call.
// A failing backend must preserve its state as required by IndexBackend.
func (b *CountBatch) Commit() error {
	if err := b.ready(); err != nil {
		return err
	}
	b.closed = true
	changes := make([]IndexChange, 0, len(b.tx.changed)+len(b.extra))
	for id, e := range b.tx.changed {
		change := IndexChange{Key: countPageKey(id), Delete: e == nil}
		if e != nil {
			if e.Refs == 0 {
				return b.fail(errors.New("unowned staged count node"))
			}
			change.Value = e.encode()
		}
		changes = append(changes, change)
	}
	for _, change := range b.extra {
		changes = append(changes, change)
	}
	for _, change := range changes {
		b.tx.stats.Writes++
		b.tx.stats.MutationBytes += len(change.Value)
		b.tx.stats.KeyBytes += len(change.Key)
	}
	return b.fail(b.tx.store.backend.Apply(changes))
}

// stageIndex is shared by outer pages and owning-root publication. Count pages
// use the typed overlay; external index code cannot overwrite their namespace.
func (b *CountBatch) stageIndex(key string, value []byte, remove bool) error {
	if err := b.ready(); err != nil {
		return err
	}
	if key == "" || len(key) >= 6 && key[:6] == "count/" {
		return b.fail(errors.New("reserved index key"))
	}
	b.extra[key] = IndexChange{Key: key, Value: bytes.Clone(value), Delete: remove}
	return nil
}

func (b *CountBatch) readIndex(key string) ([]byte, bool, error) {
	if err := b.ready(); err != nil {
		return nil, false, err
	}
	b.tx.stats.Reads++
	if change, ok := b.extra[key]; ok {
		return bytes.Clone(change.Value), !change.Delete, nil
	}
	b.tx.stats.Reads++
	value, ok, err := b.tx.store.backend.Read(key)
	if err != nil {
		return nil, false, b.fail(err)
	}
	return value, ok, nil
}
