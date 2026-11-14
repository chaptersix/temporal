package packedstore

import (
	"bytes"
	"errors"
	"math"
	"slices"
)

func (b *Batch) save(id ID, pid uint64, p page) (entry, error) {
	if len(p.entries) == 0 || len(p.entries) > 16 {
		return entry{}, b.fail(errors.New("packed page occupancy"))
	}
	for i := 1; i < len(p.entries); i++ {
		if bytes.Compare(p.entries[i-1].key, p.entries[i].key) >= 0 {
			return entry{}, b.fail(errors.New("unordered packed save"))
		}
	}
	e, err := summary(pid, p)
	if err != nil {
		return entry{}, b.fail(err)
	}
	wire := encode(p)
	b.observeWrite(len(p.entries), len(wire))
	b.put(key(id, pid), wire)
	return e, nil
}
func (b *Batch) saveSplit(id ID, c *control, pid uint64, p page) ([]entry, error) {
	if len(p.entries) <= 16 {
		e, err := b.save(id, pid, p)
		return []entry{e}, err
	}
	rightID, err := b.allocate(id, c)
	if err != nil {
		return nil, err
	}
	left := page{leaf: p.leaf, entries: p.entries[:8]}
	right := page{leaf: p.leaf, entries: p.entries[8:]}
	a, err := b.save(id, pid, left)
	if err != nil {
		return nil, err
	}
	z, err := b.save(id, rightID, right)
	return []entry{a, z}, err
}
func (b *Batch) insert(id ID, c *control, pid uint64, p page, k, value []byte, depth int, upper []byte) ([]entry, bool, error) {
	if depth >= 64 {
		return nil, false, b.fail(errors.New("packed depth exceeded"))
	}
	inserted := false
	if p.leaf {
		at, found := slices.BinarySearchFunc(p.entries, k, func(e entry, k []byte) int { return bytes.Compare(e.key, k) })
		if found {
			p.entries[at].value = bytes.Clone(value)
		} else {
			p.entries = slices.Insert(p.entries, at, entry{key: bytes.Clone(k), value: bytes.Clone(value)})
			inserted = true
		}
	} else {
		at := childPosition(p, k)
		child, err := b.boundedChild(id, p.entries[at], childUpper(p, at, upper), c.next)
		if err != nil {
			return nil, false, err
		}
		changed, added, err := b.insert(id, c, p.entries[at].child, child, k, value, depth+1, childUpper(p, at, upper))
		if err != nil {
			return nil, false, err
		}
		inserted = added
		p.entries = slices.Replace(p.entries, at, at+1, changed...)
	}
	changes, err := b.saveSplit(id, c, pid, p)
	return changes, inserted, err
}
func (b *Batch) Put(id ID, k, value []byte) error {
	c, err := b.loadControl(id)
	if err != nil {
		return err
	}
	if c.root == 0 {
		c.root, err = b.allocate(id, &c)
		if err != nil {
			return err
		}
		_, err = b.save(id, c.root, page{leaf: true, entries: []entry{{key: bytes.Clone(k), value: bytes.Clone(value)}}})
		if err != nil {
			return err
		}
		c.count = 1
		b.saveControl(id, c)
		return nil
	}
	p, err := b.root(id, c)
	if err != nil {
		return err
	}
	changed, inserted, err := b.insert(id, &c, c.root, p, k, value, 0, nil)
	if err != nil {
		return err
	}
	if inserted {
		if c.count == math.MaxUint64 {
			return b.fail(errors.New("packed record count exhausted"))
		}
		c.count++
	}
	if len(changed) == 2 {
		c.root, err = b.allocate(id, &c)
		if err != nil {
			return err
		}
		if _, err = b.save(id, c.root, page{entries: changed}); err != nil {
			return err
		}
	}
	b.saveControl(id, c)
	return nil
}
func (b *Batch) rebalance(id ID, p *page, at int, upper []byte, next uint64) error {
	if len(p.entries) < 2 {
		return nil
	}
	child, err := b.boundedChild(id, p.entries[at], childUpper(*p, at, upper), next)
	if err != nil {
		return err
	}
	if len(child.entries) >= 8 {
		return nil
	}
	left := at
	if left == len(p.entries)-1 {
		left--
	}
	a, err := b.boundedChild(id, p.entries[left], childUpper(*p, left, upper), next)
	if err != nil {
		return err
	}
	z, err := b.boundedChild(id, p.entries[left+1], childUpper(*p, left+1, upper), next)
	if err != nil {
		return err
	}
	if a.leaf != z.leaf {
		return b.fail(errors.New("packed sibling kinds differ"))
	}
	combined := append(slices.Clone(a.entries), z.entries...)
	if len(combined) <= 16 {
		e, err := b.save(id, p.entries[left].child, page{leaf: a.leaf, entries: combined})
		if err != nil {
			return err
		}
		b.remove(key(id, p.entries[left+1].child))
		p.entries = slices.Replace(p.entries, left, left+2, e)
		return nil
	}
	middle := len(combined) / 2
	first, err := b.save(id, p.entries[left].child, page{leaf: a.leaf, entries: combined[:middle]})
	if err != nil {
		return err
	}
	second, err := b.save(id, p.entries[left+1].child, page{leaf: a.leaf, entries: combined[middle:]})
	if err != nil {
		return err
	}
	p.entries[left], p.entries[left+1] = first, second
	return nil
}
func (b *Batch) deleteKey(id ID, pid uint64, p page, k []byte, depth int, upper []byte, next uint64) (*entry, bool, error) {
	if depth >= 64 {
		return nil, false, b.fail(errors.New("packed depth exceeded"))
	}
	if p.leaf {
		at, found := slices.BinarySearchFunc(p.entries, k, func(e entry, k []byte) int { return bytes.Compare(e.key, k) })
		if !found {
			e, err := summary(pid, p)
			return &e, false, err
		}
		p.entries = slices.Delete(p.entries, at, at+1)
	} else {
		at := childPosition(p, k)
		child, err := b.boundedChild(id, p.entries[at], childUpper(p, at, upper), next)
		if err != nil {
			return nil, false, err
		}
		changed, removed, err := b.deleteKey(id, p.entries[at].child, child, k, depth+1, childUpper(p, at, upper), next)
		if err != nil {
			return nil, false, err
		}
		if !removed {
			e, err := summary(pid, p)
			return &e, false, err
		}
		if changed == nil {
			p.entries = slices.Delete(p.entries, at, at+1)
		} else {
			p.entries[at] = *changed
			if err = b.rebalance(id, &p, at, upper, next); err != nil {
				return nil, false, err
			}
		}
	}
	if len(p.entries) == 0 {
		b.remove(key(id, pid))
		return nil, true, nil
	}
	e, err := b.save(id, pid, p)
	return &e, true, err
}
func (b *Batch) Delete(id ID, k []byte) error {
	c, err := b.loadControl(id)
	if err != nil || c.root == 0 {
		return err
	}
	p, err := b.root(id, c)
	if err != nil {
		return err
	}
	changed, removed, err := b.deleteKey(id, c.root, p, k, 0, nil, c.next)
	if err != nil {
		return b.fail(err)
	}
	if !removed {
		return nil
	}
	c.count--
	if changed == nil {
		c.root = 0
	} else {
		p, err = b.load(id, c.root)
		if err != nil {
			return err
		}
		if !p.leaf && len(p.entries) == 1 {
			b.remove(key(id, c.root))
			c.root = p.entries[0].child
		}
	}
	b.saveControl(id, c)
	return nil
}
