// Package packedstore is an experimental mutable storage capability. It is not
// selected by any Scheduler runtime profile.
package packedstore

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"

	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

type ID [16]byte
type entry struct {
	key, value   []byte
	child, count uint64
}
type page struct {
	leaf    bool
	entries []entry
}
type control struct{ root, next, count uint64 }

// Batch stages all namespaces against one atomic backend Apply boundary.
type Batch struct {
	backend queueindex.IndexBackend
	changes map[string]queueindex.IndexChange
	err     error
	closed  bool
}

func NewBatch(backend queueindex.IndexBackend) *Batch {
	return &Batch{backend: backend, changes: map[string]queueindex.IndexChange{}}
}
func (b *Batch) fail(err error) error {
	if b.err == nil {
		b.err = err
	}
	return b.err
}
func (b *Batch) ready() error {
	if b.closed {
		return b.fail(errors.New("packed batch closed"))
	}
	return b.err
}
func (b *Batch) read(key string) ([]byte, bool, error) {
	if err := b.ready(); err != nil {
		return nil, false, err
	}
	if c, ok := b.changes[key]; ok {
		return bytes.Clone(c.Value), !c.Delete, nil
	}
	value, ok, err := b.backend.Read(key)
	if err != nil {
		return nil, false, b.fail(err)
	}
	return bytes.Clone(value), ok, nil
}
func (b *Batch) put(key string, value []byte) {
	b.changes[key] = queueindex.IndexChange{Key: key, Value: bytes.Clone(value)}
}
func (b *Batch) remove(key string) { b.changes[key] = queueindex.IndexChange{Key: key, Delete: true} }

// Trailing zero bytes are canonical padding of the fixed128-bit ID, not lost
// identity. Neither namespace nor ordinal encoding is used for logical ordering.
func key(id ID, pageID uint64) string {
	namespace := base64.RawURLEncoding.EncodeToString(bytes.TrimRight(id[:], "\x00"))
	return "p/" + namespace + "/" + strconv.FormatUint(pageID, 16)
}
func decodeKey(encoded string) (ID, uint64, error) {
	var id ID
	parts := strings.Split(encoded, "/")
	if len(parts) != 3 || parts[0] != "p" {
		return id, 0, errors.New("invalid packed address")
	}
	namespace, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(namespace) > len(id) {
		return id, 0, errors.New("invalid packed namespace")
	}
	copy(id[:], namespace)
	ordinal, err := strconv.ParseUint(parts[2], 16, 64)
	if err != nil || key(id, ordinal) != encoded {
		return ID{}, 0, errors.New("noncanonical packed address")
	}
	return id, ordinal, nil
}
func (b *Batch) loadControl(id ID) (control, error) {
	wire, ok, err := b.read(key(id, 0))
	if err != nil {
		return control{}, err
	}
	if !ok {
		return control{next: 1}, nil
	}
	b.observeRead(1, len(wire))
	if len(wire) != 25 || wire[0] != 1 {
		return control{}, b.fail(errors.New("invalid packed control"))
	}
	c := control{root: binary.BigEndian.Uint64(wire[1:]), next: binary.BigEndian.Uint64(wire[9:]), count: binary.BigEndian.Uint64(wire[17:])}
	if c.next == 0 || c.root >= c.next || (c.root == 0) != (c.count == 0) {
		return control{}, b.fail(errors.New("inconsistent packed control"))
	}
	return c, nil
}
func (b *Batch) saveControl(id ID, c control) {
	wire := []byte{1}
	wire = binary.BigEndian.AppendUint64(wire, c.root)
	wire = binary.BigEndian.AppendUint64(wire, c.next)
	wire = binary.BigEndian.AppendUint64(wire, c.count)
	b.observeWrite(1, len(wire))
	b.put(key(id, 0), wire)
}
func (b *Batch) allocate(store ID, c *control) (uint64, error) {
	if c.next == math.MaxUint64 {
		return 0, b.fail(errors.New("packed page IDs exhausted"))
	}
	id := c.next
	if _, exists, err := b.read(key(store, id)); err != nil {
		return 0, err
	} else if exists {
		return 0, b.fail(errors.New("packed allocator target already exists"))
	}
	c.next++
	return id, nil
}
func appendBlob(dst, value []byte) []byte {
	return append(binary.AppendUvarint(dst, uint64(len(value))), value...)
}
func takeNumber(wire *[]byte) (uint64, error) {
	v, n := binary.Uvarint(*wire)
	if n <= 0 {
		return 0, errors.New("invalid packed integer")
	}
	*wire = (*wire)[n:]
	return v, nil
}
func takeBlob(wire *[]byte) ([]byte, error) {
	v, err := takeNumber(wire)
	if err != nil || v > uint64(len(*wire)) {
		return nil, errors.New("invalid packed blob")
	}
	out := bytes.Clone((*wire)[:int(v)])
	*wire = (*wire)[int(v):]
	return out, nil
}
func encode(p page) []byte {
	kind := byte(0)
	if p.leaf {
		kind = 1
	}
	out := []byte{1, kind, byte(len(p.entries))}
	prefix := p.entries[0].key
	for _, e := range p.entries[1:] {
		n := 0
		for n < len(prefix) && n < len(e.key) && prefix[n] == e.key[n] {
			n++
		}
		prefix = prefix[:n]
	}
	out = appendBlob(out, prefix)
	for _, e := range p.entries {
		out = appendBlob(out, e.key[len(prefix):])
		if p.leaf {
			out = appendBlob(out, e.value)
		} else {
			out = binary.AppendUvarint(out, e.child)
			out = binary.AppendUvarint(out, e.count)
		}
	}
	return out
}
func decode(wire []byte) (page, error) {
	if len(wire) < 3 || wire[0] != 1 || wire[1] > 1 || wire[2] == 0 || wire[2] > 16 {
		return page{}, errors.New("invalid packed page")
	}
	p := page{leaf: wire[1] == 1}
	n := int(wire[2])
	wire = wire[3:]
	prefix, err := takeBlob(&wire)
	if err != nil {
		return page{}, err
	}
	for range n {
		suffix, err := takeBlob(&wire)
		if err != nil {
			return page{}, err
		}
		e := entry{key: append(bytes.Clone(prefix), suffix...)}
		if p.leaf {
			e.value, err = takeBlob(&wire)
		} else {
			e.child, err = takeNumber(&wire)
			if err == nil {
				e.count, err = takeNumber(&wire)
			}
			if e.child == 0 || e.count == 0 {
				return page{}, errors.New("invalid packed child")
			}
		}
		if err != nil {
			return page{}, err
		}
		if len(p.entries) > 0 && bytes.Compare(p.entries[len(p.entries)-1].key, e.key) >= 0 {
			return page{}, errors.New("unordered packed keys")
		}
		p.entries = append(p.entries, e)
	}
	if len(wire) != 0 {
		return page{}, errors.New("trailing packed page")
	}
	return p, nil
}
func (b *Batch) load(id ID, pid uint64) (page, error) {
	wire, ok, err := b.read(key(id, pid))
	if err != nil {
		return page{}, err
	}
	if !ok {
		return page{}, b.fail(errors.New("missing packed page"))
	}
	p, err := decode(wire)
	b.observeRead(len(p.entries), len(wire))
	if err != nil {
		return page{}, b.fail(err)
	}
	return p, nil
}
func summary(pid uint64, p page) (entry, error) {
	e := entry{key: bytes.Clone(p.entries[0].key), child: pid}
	if p.leaf {
		e.count = uint64(len(p.entries))
		return e, nil
	}
	for _, child := range p.entries {
		if math.MaxUint64-e.count < child.count {
			return entry{}, errors.New("packed count overflow")
		}
		e.count += child.count
	}
	return e, nil
}
func (b *Batch) child(id ID, e entry) (page, error) {
	p, err := b.load(id, e.child)
	if err != nil {
		return page{}, err
	}
	actual, err := summary(e.child, p)
	if err != nil {
		return page{}, b.fail(err)
	}
	if actual.count != e.count || !bytes.Equal(actual.key, e.key) {
		return page{}, b.fail(errors.New("packed child summary mismatch"))
	}
	return p, nil
}
func childPosition(p page, k []byte) int {
	n, _ := slices.BinarySearchFunc(p.entries, k, func(e entry, k []byte) int { return bytes.Compare(e.key, k) })
	if n < len(p.entries) && bytes.Equal(p.entries[n].key, k) {
		return n
	}
	if n == 0 {
		return 0
	}
	return n - 1
}
func (b *Batch) root(id ID, c control) (page, error) {
	p, err := b.load(id, c.root)
	if err == nil {
		err = b.checkBounds(p, nil, c.next)
	}
	if err != nil {
		return page{}, err
	}
	e, err := summary(c.root, p)
	if err != nil {
		return page{}, b.fail(err)
	}
	if e.count != c.count {
		return page{}, b.fail(errors.New("packed root count mismatch"))
	}
	return p, nil
}
func (b *Batch) Count(id ID) (uint64, error) {
	c, err := b.loadControl(id)
	if err == nil && c.root != 0 {
		_, err = b.root(id, c)
	}
	return c.count, err
}
func (b *Batch) Get(id ID, k []byte) ([]byte, bool, error) {
	c, err := b.loadControl(id)
	if err != nil || c.root == 0 {
		return nil, false, err
	}
	p, err := b.root(id, c)
	var upper []byte
	for depth := 0; err == nil && depth < 64; depth++ {
		if p.leaf {
			at, ok := slices.BinarySearchFunc(p.entries, k, func(e entry, k []byte) int { return bytes.Compare(e.key, k) })
			if !ok {
				return nil, false, nil
			}
			return bytes.Clone(p.entries[at].value), true, nil
		}
		at := childPosition(p, k)
		upper = childUpper(p, at, upper)
		p, err = b.boundedChild(id, p.entries[at], upper, c.next)
	}
	if err == nil {
		err = b.fail(errors.New("packed depth exceeded"))
	}
	return nil, false, err
}
func (b *Batch) Commit() error {
	if err := b.ready(); err != nil {
		return err
	}
	changes := make([]queueindex.IndexChange, 0, len(b.changes))
	for _, c := range b.changes {
		changes = append(changes, c)
	}
	slices.SortFunc(changes, func(a, c queueindex.IndexChange) int { return bytes.Compare([]byte(a.Key), []byte(c.Key)) })
	b.closed = true
	if err := b.backend.Apply(changes); err != nil {
		return b.fail(err)
	}
	return nil
}
func (b *Batch) Abort() { b.closed = true; b.changes = nil }

// checkBounds validates the touched page against its parent's exclusive upper
// bound and allocation frontier. Descendant checks happen when they are touched.
func (b *Batch) checkBounds(p page, upper []byte, next uint64) error {
	if upper != nil && bytes.Compare(p.entries[len(p.entries)-1].key, upper) >= 0 {
		return b.fail(errors.New("packed child crosses upper separator"))
	}
	if !p.leaf {
		for _, e := range p.entries {
			if e.child >= next {
				return b.fail(errors.New("packed child exceeds allocation frontier"))
			}
		}
	}
	return nil
}
func childUpper(p page, at int, inherited []byte) []byte {
	if at+1 < len(p.entries) {
		return p.entries[at+1].key
	}
	return inherited
}
func (b *Batch) boundedChild(id ID, e entry, upper []byte, next uint64) (page, error) {
	if e.child >= next {
		return page{}, b.fail(errors.New("packed child exceeds allocation frontier"))
	}
	p, err := b.child(id, e)
	if err == nil {
		err = b.checkBounds(p, upper, next)
	}
	return p, err
}
