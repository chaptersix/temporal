package queueindex

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"slices"
)

// RetirementKey orders legacy signed nominal time, then the generated operation
// ID suffix. The common execution incarnation is excluded by the caller.
type RetirementKey [88]byte

// RetirementIndex identifies one exclusively owned retirement tree.
type RetirementIndex [16]byte

type retirementID [90]byte

type retirementNode struct {
	id          retirementID
	count       uint64
	keys        []RetirementKey
	left, right retirementID
}

const retirementBucketSize = 16

// NewRetirementKey encodes a generated operation suffix, whose current grammar
// has at most 73 nonzero ASCII bytes. Zero padding preserves lexical ordering.
func NewRetirementKey(nominal int64, suffix string) (RetirementKey, error) {
	var key RetirementKey
	if len(suffix) == 0 || len(suffix) > 73 {
		return key, errors.New("invalid retirement suffix length")
	}
	for i := range len(suffix) {
		if suffix[i] == 0 || suffix[i] > 127 {
			return key, errors.New("invalid retirement suffix byte")
		}
	}
	binary.BigEndian.PutUint64(key[:8], uint64(nominal)^(uint64(1)<<63))
	copy(key[8:], suffix)
	return key, nil
}

// Nominal returns the original legacy nominal nanosecond value.
func (key RetirementKey) Nominal() int64 {
	return int64(binary.BigEndian.Uint64(key[:8]) ^ (uint64(1) << 63))
}

// Suffix returns the generated operation ID suffix without its incarnation.
func (key RetirementKey) Suffix() string { return string(bytes.TrimRight(key[8:], "\x00")) }

func retirementValidKey(key RetirementKey) bool {
	rebuilt, err := NewRetirementKey(key.Nominal(), key.Suffix())
	return err == nil && rebuilt == key
}

func retirementCompare(a, b RetirementKey) int { return bytes.Compare(a[:], b[:]) }
func retirementDifference(a, b RetirementKey) int {
	for i := range len(a) {
		if d := a[i] ^ b[i]; d != 0 {
			return i*8 + bits.LeadingZeros8(d)
		}
	}
	return len(a) * 8
}
func retirementPrefix(key RetirementKey, depth int) retirementID {
	var id retirementID
	binary.BigEndian.PutUint16(id[:2], uint16(depth+1))
	copy(id[2:], key[:])
	for i := depth; i < 704; i++ {
		id[2+i/8] &^= 1 << uint(7-i%8)
	}
	return id
}
func (id retirementID) depth() int { return int(binary.BigEndian.Uint16(id[:2])) - 1 }
func (id retirementID) prefix() RetirementKey {
	var key RetirementKey
	copy(key[:], id[2:])
	return key
}
func retirementSide(key RetirementKey, depth int) byte { return (key[depth/8] >> uint(7-depth%8)) & 1 }
func (index RetirementIndex) rootKey() string {
	return "retirement-root/" + base64.RawURLEncoding.EncodeToString(index[:])
}
func (index RetirementIndex) pageKey(id retirementID) string {
	return "retirement/" + base64.RawURLEncoding.EncodeToString(index[:]) + "/" + base64.RawURLEncoding.EncodeToString(id[:])
}

func retirementBucket(keys []RetirementKey) retirementNode {
	return retirementNode{id: retirementPrefix(keys[0], retirementDifference(keys[0], keys[len(keys)-1])), count: uint64(len(keys)), keys: keys}
}
func (n retirementNode) encode() []byte {
	if len(n.keys) > 0 {
		wire := make([]byte, 2+88*len(n.keys))
		wire[0] = 1
		wire[1] = byte(len(n.keys))
		for i, key := range n.keys {
			copy(wire[2+i*88:], key[:])
		}
		return wire
	}
	wire := make([]byte, 189)
	wire[0] = 2
	binary.BigEndian.PutUint64(wire[1:9], n.count)
	copy(wire[9:99], n.left[:])
	copy(wire[99:], n.right[:])
	return wire
}
func (b *CountBatch) retirementRoot(index RetirementIndex) (retirementID, error) {
	wire, ok, err := b.readIndex(index.rootKey())
	var id retirementID
	if err != nil || !ok {
		return id, err
	}
	if len(wire) != len(id) {
		return id, b.fail(errors.New("invalid retirement root"))
	}
	copy(id[:], wire)
	if id == (retirementID{}) {
		return id, b.fail(errors.New("empty retirement root"))
	}
	return id, nil
}
func (b *CountBatch) retirementNode(index RetirementIndex, id retirementID) (retirementNode, error) {
	n := retirementNode{id: id}
	depth := id.depth()
	if depth < 0 || depth > 704 || retirementPrefix(id.prefix(), depth) != id {
		return n, b.fail(errors.New("invalid retirement prefix"))
	}
	wire, ok, err := b.readIndex(index.pageKey(id))
	if err != nil {
		return n, err
	}
	if !ok || len(wire) < 2 {
		return n, b.fail(errors.New("missing retirement node"))
	}
	switch wire[0] {
	case 1:
		return b.retirementDecodeBucket(n, wire)
	case 2:
		if len(wire) != 189 || depth >= 704 {
			return n, b.fail(errors.New("invalid retirement branch encoding"))
		}
		n.count = binary.BigEndian.Uint64(wire[1:9])
		copy(n.left[:], wire[9:99])
		copy(n.right[:], wire[99:])
		if n.count <= retirementBucketSize {
			return n, b.fail(errors.New("invalid retirement branch count"))
		}
		for side, child := range []retirementID{n.left, n.right} {
			if child.depth() <= depth || child.depth() > 704 || retirementPrefix(child.prefix(), depth) != id || retirementSide(child.prefix(), depth) != byte(side) {
				return n, b.fail(errors.New("invalid retirement child prefix"))
			}
		}
	default:
		return n, b.fail(errors.New("invalid retirement node version"))
	}
	return n, nil
}
func (b *CountBatch) retirementDecodeBucket(n retirementNode, wire []byte) (retirementNode, error) {
	size := int(wire[1])
	if size < 1 || size > retirementBucketSize || len(wire) != 2+size*88 {
		return n, b.fail(errors.New("invalid retirement bucket encoding"))
	}
	n.keys = make([]RetirementKey, size)
	n.count = uint64(size)
	for i := range size {
		copy(n.keys[i][:], wire[2+i*88:2+(i+1)*88])
		if !retirementValidKey(n.keys[i]) || i > 0 && retirementCompare(n.keys[i-1], n.keys[i]) >= 0 {
			return n, b.fail(errors.New("invalid retirement bucket order"))
		}
	}
	if retirementBucket(n.keys).id != n.id {
		return n, b.fail(errors.New("invalid retirement bucket prefix"))
	}
	return n, nil
}
func (b *CountBatch) retirementChildren(index RetirementIndex, n retirementNode) (left retirementNode, right retirementNode, err error) {
	left, err = b.retirementNode(index, n.left)
	if err != nil {
		return left, retirementNode{}, err
	}
	right, err = b.retirementNode(index, n.right)
	if err != nil {
		return left, right, err
	}
	if left.count > math.MaxUint64-right.count || left.count+right.count != n.count {
		return left, right, b.fail(errors.New("invalid retirement child count"))
	}
	return left, right, nil
}
func (b *CountBatch) stageRetirement(index RetirementIndex, n retirementNode) error {
	return b.stageIndex(index.pageKey(n.id), n.encode(), false)
}

// RetirementMinimum returns the exact minimum (legacy nominal, operation ID).
// It reads only a fixed-depth path and its siblings and never stages mutations.
func (b *CountBatch) RetirementMinimum(index RetirementIndex) (RetirementKey, bool, error) {
	id, err := b.retirementRoot(index)
	if err != nil || id == (retirementID{}) {
		return RetirementKey{}, false, err
	}
	n, err := b.retirementNode(index, id)
	for err == nil && len(n.keys) == 0 {
		n, _, err = b.retirementChildren(index, n)
	}
	if err != nil {
		return RetirementKey{}, false, err
	}
	return n.keys[0], true, nil
}

// InsertRetirement stages one unique StartOnly retention identity.
func (b *CountBatch) InsertRetirement(index RetirementIndex, key RetirementKey) error {
	return b.changeRetirement(index, key, true)
}

// DeleteRetirement stages removal of one existing retention identity.
func (b *CountBatch) DeleteRetirement(index RetirementIndex, key RetirementKey) error {
	return b.changeRetirement(index, key, false)
}

func (b *CountBatch) changeRetirement(index RetirementIndex, key RetirementKey, add bool) error {
	if err := b.ready(); err != nil {
		return err
	}
	if !retirementValidKey(key) {
		return b.fail(errors.New("invalid retirement key"))
	}
	id, err := b.retirementRoot(index)
	if err != nil {
		return err
	}
	if id == (retirementID{}) {
		if !add {
			return b.fail(errors.New("missing retirement key"))
		}
		n := retirementBucket([]RetirementKey{key})
		if err = b.stageRetirement(index, n); err != nil {
			return err
		}
		return b.stageIndex(index.rootKey(), n.id[:], false)
	}
	n, err := b.retirementNode(index, id)
	if err != nil {
		return err
	}
	next, err := b.retirementChange(index, n, key, add)
	if err != nil {
		return b.fail(err)
	}
	return b.stageIndex(index.rootKey(), next.id[:], next.count == 0)
}
func (b *CountBatch) retirementChange(index RetirementIndex, n retirementNode, key RetirementKey, add bool) (retirementNode, error) {
	if len(n.keys) > 0 {
		return b.retirementChangeBucket(index, n, key, add)
	}
	left, right, err := b.retirementChildren(index, n)
	if err != nil {
		return n, err
	}
	if add && n.count == math.MaxUint64 {
		return n, errors.New("retirement count overflow")
	}
	difference := retirementDifference(key, n.id.prefix())
	if difference < n.id.depth() {
		if !add {
			return n, errors.New("missing retirement key")
		}
		leaf := retirementBucket([]RetirementKey{key})
		branch := retirementNode{id: retirementPrefix(key, difference), count: n.count + 1, left: leaf.id, right: n.id}
		if retirementSide(key, difference) != 0 {
			branch.left, branch.right = branch.right, branch.left
		}
		if err = b.stageRetirement(index, leaf); err == nil {
			err = b.stageRetirement(index, branch)
		}
		return branch, err
	}
	if retirementSide(key, n.id.depth()) == 0 {
		left, err = b.retirementChange(index, left, key, add)
	} else {
		right, err = b.retirementChange(index, right, key, add)
	}
	if err != nil {
		return n, err
	}
	if err = b.stageIndex(index.pageKey(n.id), nil, true); err != nil {
		return n, err
	}
	if left.count == 0 {
		return right, nil
	}
	if right.count == 0 {
		return left, nil
	}
	if left.count > math.MaxUint64-right.count {
		return n, errors.New("retirement count overflow")
	}
	n.count = left.count + right.count
	n.left, n.right = left.id, right.id
	if n.count <= retirementBucketSize {
		if len(left.keys) == 0 || len(right.keys) == 0 {
			return n, errors.New("invalid retirement collapse")
		}
		if err = b.stageIndex(index.pageKey(left.id), nil, true); err != nil {
			return n, err
		}
		if err = b.stageIndex(index.pageKey(right.id), nil, true); err != nil {
			return n, err
		}
		n = retirementBucket(append(slices.Clone(left.keys), right.keys...))
	}
	return n, b.stageRetirement(index, n)
}
func (b *CountBatch) retirementChangeBucket(index RetirementIndex, n retirementNode, key RetirementKey, add bool) (retirementNode, error) {
	pos, found := slices.BinarySearchFunc(n.keys, key, retirementCompare)
	if add == found {
		return n, errors.New("retirement key presence mismatch")
	}
	keys := slices.Clone(n.keys)
	if add {
		keys = slices.Insert(keys, pos, key)
	} else {
		keys = slices.Delete(keys, pos, pos+1)
	}
	if err := b.stageIndex(index.pageKey(n.id), nil, true); err != nil {
		return n, err
	}
	if len(keys) == 0 {
		return retirementNode{}, nil
	}
	next := retirementBucket(keys)
	if len(keys) <= retirementBucketSize {
		return next, b.stageRetirement(index, next)
	}
	split := 0
	for retirementSide(keys[split], next.id.depth()) == 0 {
		split++
	}
	left, right := retirementBucket(keys[:split]), retirementBucket(keys[split:])
	next.keys = nil
	next.left, next.right = left.id, right.id
	if err := b.stageRetirement(index, left); err != nil {
		return n, err
	}
	if err := b.stageRetirement(index, right); err != nil {
		return n, err
	}
	return next, b.stageRetirement(index, next)
}
