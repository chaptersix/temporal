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
type CountStore struct{ nodes map[CountRoot]countEntry }

// NewCountStore constructs an empty count index store.
func NewCountStore() *CountStore { return &CountStore{nodes: make(map[CountRoot]countEntry)} }

// EncodedSize reports canonical page plus reference-count bytes only. CHASM node
// paths, protobuf envelopes, maps and database overhead are deliberately excluded.
func (s *CountStore) EncodedSize() int { return len(s.nodes) * (86 + 8) }

// Fork retains a second owner of root without copying its descendants.
func (s *CountStore) Fork(root CountRoot) error {
	if root == (CountRoot{}) {
		return nil
	}
	e, ok := s.nodes[root]
	if !ok {
		return errors.New("missing count root")
	}
	if e.Refs == math.MaxUint64 {
		return errors.New("count references exhausted")
	}
	e.Refs++
	s.nodes[root] = e
	return nil
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
	e, ok := t.store.nodes[id]
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
	if existing, ok := t.changed[id]; ok && existing != nil {
		if existing.Node != n {
			return CountRoot{}, errors.New("count identity collision")
		}
		return id, nil
	}
	t.stats.Reads++
	if existing, ok := t.store.nodes[id]; ok {
		if existing.Node != n {
			return CountRoot{}, errors.New("count identity collision")
		}
		return id, nil
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

func (t *countTxn) change(root CountRoot, key CountKey, add bool) (CountRoot, error) {
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
	if keyBit(key, n.Bit) == 0 {
		n.Left, err = t.change(n.Left, key, add)
	} else {
		n.Right, err = t.change(n.Right, key, add)
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

func (t *countTxn) release(id CountRoot) error {
	if id == (CountRoot{}) {
		return nil
	}
	e, err := t.read(id)
	if err != nil {
		return err
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
	t.changed[id] = nil
	if err = t.release(e.Node.Left); err != nil {
		return err
	}
	return t.release(e.Node.Right)
}

// Update atomically replaces one owned root reference after adding/removing one
// occurrence. Failure leaves every committed page and reference unchanged.
func (s *CountStore) Update(root CountRoot, key CountKey, add bool) (CountRoot, CountStats, error) {
	t := &countTxn{store: s, changed: make(map[CountRoot]*countEntry)}
	next, err := t.change(root, key, add)
	if err == nil {
		err = t.retain(next)
	}
	if err == nil {
		err = t.release(root)
	}
	if err != nil {
		return root, t.stats, err
	}
	for id, e := range t.changed {
		t.stats.Writes++
		if e == nil {
			delete(s.nodes, id)
		} else {
			s.nodes[id] = *e
		}
	}
	return next, t.stats, nil
}

// Count returns the number of occurrences at an owned root.
func (s *CountStore) Count(root CountRoot) (uint64, error) {
	if root == (CountRoot{}) {
		return 0, nil
	}
	e, ok := s.nodes[root]
	if !ok {
		return 0, errors.New("missing count root")
	}
	return e.Node.Count, nil
}

// CountBefore returns multiplicity strictly before key and metadata node reads.
func (s *CountStore) CountBefore(root CountRoot, key CountKey) (uint64, int, error) {
	var count uint64
	reads := 0
	for root != (CountRoot{}) {
		e, ok := s.nodes[root]
		reads++
		if !ok {
			return 0, reads, errors.New("missing count node")
		}
		n := e.Node
		if firstDifferent(n.Key, key) < n.Bit || n.Bit == 96 {
			if bytes.Compare(n.Key[:], key[:]) < 0 {
				count += n.Count
			}
			return count, reads, nil
		}
		if keyBit(key, n.Bit) == 0 {
			root = n.Left
		} else {
			l, ok := s.nodes[n.Left]
			reads++
			if !ok {
				return 0, reads, errors.New("missing count child")
			}
			count += l.Node.Count
			root = n.Right
		}
	}
	return count, reads, nil
}
