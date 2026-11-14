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

const countBucketSize = 16
const countHeaderBytes = 94
const countMaxNodeBytes = 2 + 22*countBucketSize

type countItem struct {
	Key   CountKey
	Count uint64
}

type countNode struct {
	Key         CountKey
	Bit         uint8 // Bit96 denotes a canonical sorted bucket of at most16 distinct keys.
	Count       uint64
	Distinct    uint64
	Left, Right CountRoot
	Items       [countBucketSize]countItem
}

func countLeaf(key CountKey, count uint64) countNode {
	return countNode{Key: key, Bit: 96, Count: count, Distinct: 1, Items: [countBucketSize]countItem{{Key: key, Count: count}}}
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
	wire := binary.AppendUvarint([]byte{6}, e.Refs)
	return append(wire, e.Node.encode()...)
}

// canonicalUint rejects alternate encodings so stored pages have one wire form.
// binary.Uvarint examines at most ten bytes for a uint64.
func canonicalUint(wire []byte) (uint64, int, error) {
	value, size := binary.Uvarint(wire)
	if size <= 0 || size > 1 && wire[size-1] == 0 {
		return 0, 0, errors.New("invalid canonical unsigned integer")
	}
	return value, size, nil
}

func decodeCount(id CountRoot, wire []byte) (countEntry, error) {
	if len(wire) < 4 || len(wire) > countMaxNodeBytes+11 || wire[0] != 6 {
		return countEntry{}, errors.New("invalid count page encoding")
	}
	refs, size, err := canonicalUint(wire[1:])
	if err != nil || refs == 0 {
		return countEntry{}, errors.New("invalid count page references")
	}
	nodeWire := wire[1+size:]
	if len(nodeWire) < 2 || sha256.Sum256(nodeWire) != id {
		return countEntry{}, errors.New("count page identity mismatch")
	}
	var n countNode
	switch nodeWire[0] {
	case 4:
		n, err = decodeCountBranch(nodeWire)
	case 7:
		n, err = decodeCountBucket(nodeWire)
	default:
		err = errors.New("invalid count page schema")
	}
	if err != nil {
		return countEntry{}, err
	}
	return countEntry{Node: n, Refs: refs}, nil
}

func decodeCountBranch(wire []byte) (countNode, error) {
	if len(wire) != countHeaderBytes || wire[1] >= 96 {
		return countNode{}, errors.New("invalid count branch encoding")
	}
	n := countNode{Bit: wire[1], Count: binary.BigEndian.Uint64(wire[14:22]), Distinct: binary.BigEndian.Uint64(wire[86:94])}
	copy(n.Key[:], wire[2:14])
	copy(n.Left[:], wire[22:54])
	copy(n.Right[:], wire[54:86])
	if n.Distinct <= countBucketSize || n.Distinct > n.Count || n.Left == (CountRoot{}) || n.Right == (CountRoot{}) {
		return countNode{}, errors.New("invalid count branch counters")
	}
	return n, nil
}

func decodeCountBucket(wire []byte) (countNode, error) {
	if len(wire) < 2 {
		return countNode{}, errors.New("invalid count bucket encoding")
	}
	distinct := int(wire[1])
	if distinct == 0 || distinct > countBucketSize {
		return countNode{}, errors.New("invalid count bucket encoding")
	}
	n := countNode{Bit: 96, Distinct: uint64(distinct)}
	offset := 2
	for i := range distinct {
		if len(wire)-offset < 13 {
			return countNode{}, errors.New("invalid count bucket encoding")
		}
		copy(n.Items[i].Key[:], wire[offset:offset+12])
		count, size, err := canonicalUint(wire[offset+12:])
		if err != nil {
			return countNode{}, err
		}
		n.Items[i].Count = count
		offset += 12 + size
		item := n.Items[i]
		if item.Count == 0 || n.Count > math.MaxUint64-item.Count || i > 0 && bytes.Compare(n.Items[i-1].Key[:], item.Key[:]) >= 0 {
			return countNode{}, errors.New("invalid count bucket ordering")
		}
		n.Count += item.Count
	}
	if offset != len(wire) {
		return countNode{}, errors.New("trailing count bucket bytes")
	}
	n.Key = n.Items[0].Key
	return n, nil
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
	created []CountRoot
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

func (n countNode) encode() []byte {
	if n.Bit == 96 {
		distinct := int(min(n.Distinct, countBucketSize))
		wire := make([]byte, 2, 2+22*distinct)
		wire[0], wire[1] = 7, byte(distinct)
		for i := range distinct {
			wire = append(wire, n.Items[i].Key[:]...)
			wire = binary.AppendUvarint(wire, n.Items[i].Count)
		}
		return wire
	}
	wire := make([]byte, countHeaderBytes)
	wire[0], wire[1] = 4, n.Bit
	copy(wire[2:14], n.Key[:])
	binary.BigEndian.PutUint64(wire[14:22], n.Count)
	copy(wire[22:54], n.Left[:])
	copy(wire[54:86], n.Right[:])
	binary.BigEndian.PutUint64(wire[86:94], n.Distinct)
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
	t.created = append(t.created, id)
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
	if l.Node.Distinct > math.MaxUint64-r.Node.Distinct {
		return CountRoot{}, errors.New("distinct count exhausted")
	}
	distinct := l.Node.Distinct + r.Node.Distinct
	if distinct <= countBucketSize {
		if l.Node.Bit != 96 || r.Node.Bit != 96 {
			return CountRoot{}, errors.New("noncanonical small count branch")
		}
		items := make([]countItem, 0, int(distinct))
		items = append(items, l.Node.Items[:int(l.Node.Distinct)]...)
		items = append(items, r.Node.Items[:int(r.Node.Distinct)]...)
		return t.bucket(items)
	}
	return t.intern(countNode{Key: l.Node.Key, Bit: at, Count: l.Node.Count + r.Node.Count, Distinct: distinct, Left: left, Right: right})
}

func bucketCrossesBit(n countNode, bit uint8) bool {
	return n.Bit == 96 && firstDifferent(n.Key, n.Items[int(n.Distinct)-1].Key) <= bit
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
	if n.Bit >= 96 || bucketCrossesBit(l.Node, n.Bit) || bucketCrossesBit(r.Node, n.Bit) || l.Node.Bit <= n.Bit || r.Node.Bit <= n.Bit || n.Key != l.Node.Key || firstDifferent(l.Node.Key, r.Node.Key) != n.Bit || keyBit(l.Node.Key, n.Bit) != 0 || keyBit(r.Node.Key, n.Bit) != 1 || l.Node.Count > math.MaxUint64-r.Node.Count || n.Count != l.Node.Count+r.Node.Count || l.Node.Distinct > math.MaxUint64-r.Node.Distinct || n.Distinct != l.Node.Distinct+r.Node.Distinct {
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
		return t.intern(countLeaf(key, 1))
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
	if n.Bit == 96 {
		return t.changeBucket(n, key, add)
	}
	different := firstDifferent(n.Key, key)
	if different < n.Bit {
		if !add {
			return CountRoot{}, errors.New("count key absent")
		}
		leaf, err := t.intern(countLeaf(key, 1))
		if err != nil {
			return CountRoot{}, err
		}
		if keyBit(key, different) == 0 {
			return t.branch(leaf, root, different)
		}
		return t.branch(root, leaf, different)
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

func (t *countTxn) bucket(items []countItem) (CountRoot, error) {
	if len(items) == 0 {
		return CountRoot{}, nil
	}
	if len(items) > countBucketSize {
		at := firstDifferent(items[0].Key, items[len(items)-1].Key)
		split := 0
		for split < len(items) && keyBit(items[split].Key, at) == 0 {
			split++
		}
		left, err := t.bucket(items[:split])
		if err != nil {
			return CountRoot{}, err
		}
		right, err := t.bucket(items[split:])
		if err != nil {
			return CountRoot{}, err
		}
		return t.branch(left, right, at)
	}
	n := countNode{Key: items[0].Key, Bit: 96, Distinct: uint64(len(items))}
	for i, item := range items {
		if n.Count > math.MaxUint64-item.Count {
			return CountRoot{}, errors.New("count exhausted")
		}
		n.Items[i] = item
		n.Count += item.Count
	}
	return t.intern(n)
}

func (t *countTxn) changeBucket(n countNode, key CountKey, add bool) (CountRoot, error) {
	items := make([]countItem, int(n.Distinct), countBucketSize+1)
	copy(items, n.Items[:int(n.Distinct)])
	i := 0
	for i < len(items) && bytes.Compare(items[i].Key[:], key[:]) < 0 {
		i++
	}
	if i < len(items) && items[i].Key == key {
		if add {
			if items[i].Count == math.MaxUint64 {
				return CountRoot{}, errors.New("count exhausted")
			}
			items[i].Count++
		} else if items[i].Count == 1 {
			items = append(items[:i], items[i+1:]...)
		} else {
			items[i].Count--
		}
	} else {
		if !add {
			return CountRoot{}, errors.New("count key absent")
		}
		items = append(items, countItem{})
		copy(items[i+1:], items[i:])
		items[i] = countItem{Key: key, Count: 1}
	}
	return t.bucket(items)
}

// discardUnowned removes bounded temporary pages superseded by bucket merging.
// It scans only nodes constructed by this point update, never the whole overlay.
func (t *countTxn) discardUnowned(start int) error {
	for i := len(t.created) - 1; i >= start; i-- {
		id := t.created[i]
		t.stats.Reads++
		e := t.changed[id]
		if e == nil || e.Refs != 0 {
			continue
		}
		t.changed[id] = nil
		if err := t.release(e.Node.Left); err != nil {
			return err
		}
		if err := t.release(e.Node.Right); err != nil {
			return err
		}
	}
	return nil
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
		if n.Bit == 96 {
			for _, item := range n.Items[:int(n.Distinct)] {
				if bytes.Compare(item.Key[:], key[:]) < 0 {
					count += item.Count
				}
			}
			return count, reads, nil
		}
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
