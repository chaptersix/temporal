package queueindex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// CountKey is the chronological seconds/nanoseconds prefix of a TimeKey.
type CountKey [12]byte

// CountRoot identifies an immutable canonical Patricia node. Zero denotes empty.
type CountRoot [32]byte

// CountStats measures point-transaction metadata work, excluding map allocator overhead.
// EncodedBytes counts canonical hash-input bytes, not committed mutation bytes;
// Writes counts distinct changed entries, including refcount-only writes and deletes.
type CountStats struct {
	Reads, Writes, Releases, EncodedBytes int
	MutationBytes, KeyBytes               int
}

type countNode struct {
	Key         CountKey
	Bit         uint8 // A leaf has Bit=96; branches distinguish the indicated key bit.
	Count       uint64
	Left, Right CountRoot
}

type countEntry struct {
	Node countNode
	Refs uint64
}

// CountStore is a standalone keyed-node prototype, not a persistence adapter.
// Callers serialize access and retain one root reference for each owned handle.
type CountStore struct{ backend IndexBackend }

// NewCountStore constructs an empty count index store.
func NewCountStore() *CountStore { return NewCountStoreWithBackend(NewMemoryIndexBackend()) }

// NewCountStoreWithBackend binds the keyed storage boundary without loading it.
func NewCountStoreWithBackend(backend IndexBackend) *CountStore { return &CountStore{backend: backend} }

// Fork retains a second owner in a single standalone batch.
func (s *CountStore) Fork(root CountRoot) error {
	batch := s.Begin()
	if err := batch.Fork(root); err != nil {
		return err
	}
	return batch.Commit()
}

func (e countEntry) encode() []byte {
	node := e.Node.encode()
	wire := make([]byte, 94)
	copy(wire, node[:])
	binary.BigEndian.PutUint64(wire[86:], e.Refs)
	return wire
}

func decodeCount(id CountRoot, wire []byte) (countEntry, error) {
	if len(wire) != 94 || wire[0] != 1 || wire[1] > 96 {
		return countEntry{}, errors.New("invalid count page encoding")
	}
	if sha256.Sum256(wire[:86]) != id {
		return countEntry{}, errors.New("count page identity mismatch")
	}
	n := countNode{Bit: wire[1], Count: binary.BigEndian.Uint64(wire[14:22])}
	copy(n.Key[:], wire[2:14])
	copy(n.Left[:], wire[22:54])
	copy(n.Right[:], wire[54:86])
	e := countEntry{Node: n, Refs: binary.BigEndian.Uint64(wire[86:])}
	if n.Count == 0 || e.Refs == 0 {
		return countEntry{}, errors.New("invalid count page counters")
	}
	leaf := n.Bit == 96
	if leaf && (n.Left != (CountRoot{}) || n.Right != (CountRoot{})) || !leaf && (n.Left == (CountRoot{}) || n.Right == (CountRoot{})) {
		return countEntry{}, errors.New("invalid count page children")
	}
	return e, nil
}

func (s *CountStore) load(id CountRoot) (countEntry, bool, error) {
	wire, ok, err := s.backend.Read(countPageKey(id))
	if err != nil || !ok {
		return countEntry{}, ok, err
	}
	e, err := decodeCount(id, wire)
	return e, true, err
}

type countTxn struct {
	store   *CountStore
	changed map[CountRoot]*countEntry
	stats   CountStats
}

func (t *countTxn) read(id CountRoot) (countEntry, error) {
	t.stats.Reads++
	if e, ok := t.changed[id]; ok {
		if e == nil {
			return countEntry{}, errors.New("missing staged count node")
		}
		return *e, nil
	}
	t.stats.Reads++
	e, ok, err := t.store.load(id)
	if err != nil {
		return countEntry{}, err
	}
	if !ok {
		return countEntry{}, errors.New("missing count node")
	}
	return e, nil
}

func (t *countTxn) put(id CountRoot, e countEntry) { t.changed[id] = &e }

func (n countNode) encode() [86]byte {
	var wire [86]byte
	wire[0], wire[1] = 1, n.Bit
	copy(wire[2:14], n.Key[:])
	binary.BigEndian.PutUint64(wire[14:22], n.Count)
	copy(wire[22:54], n.Left[:])
	copy(wire[54:86], n.Right[:])
	return wire
}

func (t *countTxn) retain(id CountRoot) error {
	if id == (CountRoot{}) {
		return nil
	}
	e, err := t.read(id)
	if err != nil {
		return err
	}
	if e.Refs == math.MaxUint64 {
		return errors.New("count references exhausted")
	}
	e.Refs++
	t.put(id, e)
	return nil
}

func (t *countTxn) intern(n countNode) (CountRoot, error) {
	wire := n.encode()
	id := sha256.Sum256(wire[:])
	t.stats.EncodedBytes += len(wire)
	if id == (CountRoot{}) {
		return id, errors.New("reserved count identity")
	}
	// Canonical reuse checks exact page contents, including both child identities.
	t.stats.Reads++
	existing, staged := t.changed[id]
	if staged && existing != nil {
		if existing.Node != n {
			return CountRoot{}, errors.New("count identity collision")
		}
		return id, nil
	}
	if !staged {
		t.stats.Reads++
		if existing, ok, err := t.store.load(id); err != nil {
			return CountRoot{}, err
		} else if ok {
			if existing.Node != n {
				return CountRoot{}, errors.New("count identity collision")
			}
			return id, nil
		}
	}
	if err := t.retain(n.Left); err != nil {
		return CountRoot{}, err
	}
	if err := t.retain(n.Right); err != nil {
		return CountRoot{}, err
	}
	t.put(id, countEntry{Node: n})
	return id, nil
}

func firstDifferent(a, b CountKey) uint8 {
	for i := range a {
		if x := a[i] ^ b[i]; x != 0 {
			return uint8(i*8 + bits.LeadingZeros8(x))
		}
	}
	return 96
}

func keyBit(key CountKey, at uint8) byte { return (key[at/8] >> (7 - at%8)) & 1 }

func (t *countTxn) branch(left, right CountRoot, at uint8) (CountRoot, error) {
	l, err := t.read(left)
	if err != nil {
		return CountRoot{}, err
	}
	r, err := t.read(right)
	if err != nil {
		return CountRoot{}, err
	}
	if l.Node.Count > math.MaxUint64-r.Node.Count {
		return CountRoot{}, errors.New("count exhausted")
	}
	return t.intern(countNode{Key: l.Node.Key, Bit: at, Count: l.Node.Count + r.Node.Count, Left: left, Right: right})
}

func validateCountBranch(n countNode, read func(CountRoot) (countEntry, error)) (left countEntry, right countEntry, err error) {
	l, err := read(n.Left)
	if err != nil {
		return countEntry{}, countEntry{}, err
	}
	r, err := read(n.Right)
	if err != nil {
		return countEntry{}, countEntry{}, err
	}
	if n.Bit >= 96 || l.Node.Bit <= n.Bit || r.Node.Bit <= n.Bit || n.Key != l.Node.Key || firstDifferent(l.Node.Key, r.Node.Key) != n.Bit || keyBit(l.Node.Key, n.Bit) != 0 || keyBit(r.Node.Key, n.Bit) != 1 || l.Node.Count > math.MaxUint64-r.Node.Count || n.Count != l.Node.Count+r.Node.Count {
		return countEntry{}, countEntry{}, errors.New("invalid count branch relationship")
	}
	return l, r, nil
}

func (t *countTxn) change(root CountRoot, key CountKey, add bool) (CountRoot, error) {
	return t.changeAfter(root, key, add, -1)
}

func (t *countTxn) changeAfter(root CountRoot, key CountKey, add bool, parentBit int) (CountRoot, error) {
	if root == (CountRoot{}) {
		if !add {
			return CountRoot{}, errors.New("count key absent")
		}
		return t.intern(countNode{Key: key, Bit: 96, Count: 1})
	}
	e, err := t.read(root)
	if err != nil {
		return CountRoot{}, err
	}
	n := e.Node
	if int(n.Bit) <= parentBit {
		return CountRoot{}, errors.New("invalid count branch progression")
	}
	if n.Bit < 96 {
		if _, _, err := validateCountBranch(n, t.read); err != nil {
			return CountRoot{}, err
		}
	}
	different := firstDifferent(n.Key, key)
	if different < n.Bit {
		if !add {
			return CountRoot{}, errors.New("count key absent")
		}
		leaf, err := t.intern(countNode{Key: key, Bit: 96, Count: 1})
		if err != nil {
			return CountRoot{}, err
		}
		if keyBit(key, different) == 0 {
			return t.branch(leaf, root, different)
		}
		return t.branch(root, leaf, different)
	}
	if n.Bit == 96 {
		return t.changeLeaf(n, add)
	}
	if keyBit(key, n.Bit) == 0 {
		n.Left, err = t.changeAfter(n.Left, key, add, int(n.Bit))
	} else {
		n.Right, err = t.changeAfter(n.Right, key, add, int(n.Bit))
	}
	if err != nil {
		return CountRoot{}, err
	}
	if n.Left == (CountRoot{}) {
		return n.Right, nil
	}
	if n.Right == (CountRoot{}) {
		return n.Left, nil
	}
	return t.branch(n.Left, n.Right, n.Bit)
}

func (t *countTxn) changeLeaf(n countNode, add bool) (CountRoot, error) {
	if add {
		if n.Count == math.MaxUint64 {
			return CountRoot{}, errors.New("count exhausted")
		}
		n.Count++
	} else {
		if n.Count == 1 {
			return CountRoot{}, nil
		}
		n.Count--
	}
	return t.intern(n)
}

func (t *countTxn) release(id CountRoot) error { return t.releaseAfter(id, -1) }

func (t *countTxn) releaseAfter(id CountRoot, parentBit int) error {
	if id == (CountRoot{}) {
		return nil
	}
	e, err := t.read(id)
	if err != nil {
		return err
	}
	if int(e.Node.Bit) <= parentBit {
		return errors.New("invalid count branch progression")
	}
	t.stats.Releases++
	if e.Refs == 0 {
		return errors.New("count reference underflow")
	}
	e.Refs--
	if e.Refs != 0 {
		t.put(id, e)
		return nil
	}
	if e.Node.Bit < 96 {
		if _, _, err := validateCountBranch(e.Node, t.read); err != nil {
			return err
		}
	}
	t.changed[id] = nil
	if err = t.releaseAfter(e.Node.Left, int(e.Node.Bit)); err != nil {
		return err
	}
	return t.releaseAfter(e.Node.Right, int(e.Node.Bit))
}

// Update atomically replaces one owned root reference after adding/removing one
// occurrence. Failure leaves every committed page and reference unchanged.
func (s *CountStore) Update(root CountRoot, key CountKey, add bool) (CountRoot, CountStats, error) {
	batch := s.Begin()
	next, err := batch.Update(root, key, add)
	if err == nil {
		err = batch.Commit()
	}
	if err != nil {
		return root, batch.Stats(), err
	}
	return next, batch.Stats(), nil
}

// Count returns the number of occurrences at an owned root.
func (s *CountStore) Count(root CountRoot) (uint64, error) {
	if root == (CountRoot{}) {
		return 0, nil
	}
	e, ok, err := s.load(root)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("missing count root")
	}
	if e.Node.Bit < 96 {
		read := func(id CountRoot) (countEntry, error) {
			child, ok, err := s.load(id)
			if err != nil {
				return countEntry{}, err
			}
			if !ok {
				return countEntry{}, errors.New("missing count child")
			}
			return child, nil
		}
		if _, _, err := validateCountBranch(e.Node, read); err != nil {
			return 0, err
		}
	}
	return e.Node.Count, nil
}

// CountBefore returns multiplicity strictly before key and metadata node reads.
func countBefore(root CountRoot, key CountKey, read func(CountRoot) (countEntry, error)) (uint64, int, error) {
	var count uint64
	reads := 0
	counted := func(id CountRoot) (countEntry, error) { reads++; return read(id) }
	if root == (CountRoot{}) {
		return 0, 0, nil
	}
	e, err := counted(root)
	if err != nil {
		return 0, reads, err
	}
	for {
		n := e.Node
		var l, r countEntry
		if n.Bit < 96 {
			l, r, err = validateCountBranch(n, counted)
			if err != nil {
				return 0, reads, err
			}
		}
		if firstDifferent(n.Key, key) < n.Bit || n.Bit == 96 {
			if bytes.Compare(n.Key[:], key[:]) < 0 {
				count += n.Count
			}
			return count, reads, nil
		}
		if keyBit(key, n.Bit) == 0 {
			e = l
		} else {
			count += l.Node.Count
			e = r
		}
	}
}

func (s *CountStore) CountBefore(root CountRoot, key CountKey) (uint64, int, error) {
	return countBefore(root, key, func(id CountRoot) (countEntry, error) {
		e, ok, err := s.load(id)
		if err != nil {
			return countEntry{}, err
		}
		if !ok {
			return countEntry{}, errors.New("missing count node")
		}
		return e, nil
	})
}

// CountBefore queries staged roots and poisons the batch on a missing page.
func (b *CountBatch) CountBefore(root CountRoot, key CountKey) (uint64, int, error) {
	if err := b.ready(); err != nil {
		return 0, 0, err
	}
	count, reads, err := countBefore(root, key, b.tx.read)
	if err != nil {
		return 0, reads, b.fail(err)
	}
	return count, reads, nil
}

// Count reads multiplicity through the shared overlay.
func (b *CountBatch) Count(root CountRoot) (uint64, error) {
	if err := b.ready(); err != nil {
		return 0, err
	}
	if root == (CountRoot{}) {
		return 0, nil
	}
	e, err := b.tx.read(root)
	if err != nil {
		return 0, b.fail(err)
	}
	if e.Node.Bit < 96 {
		if _, _, err := validateCountBranch(e.Node, b.tx.read); err != nil {
			return 0, b.fail(err)
		}
	}
	return e.Node.Count, nil
}
