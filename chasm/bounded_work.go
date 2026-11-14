package chasm

import (
	"iter"
	"slices"
)

type nodeWorkset struct {
	entries []*Node
	next    int
	pending map[*Node]struct{}
}

func (w *nodeWorkset) add(node *Node) {
	if w.pending == nil {
		w.pending = make(map[*Node]struct{})
	}
	if _, exists := w.pending[node]; exists {
		return
	}
	w.pending[node] = struct{}{}
	w.entries = append(w.entries, node)
	node.noteWork(workSetEnqueues, 1)
}
func (w *nodeWorkset) pop() *Node {
	if w.next == len(w.entries) {
		w.reset()
		return nil
	}
	node := w.entries[w.next]
	w.entries[w.next] = nil
	w.next++
	delete(w.pending, node)
	node.noteWork(workSetDrains, 1)
	return node
}
func (w *nodeWorkset) reset() {
	clear(w.entries)
	w.entries = w.entries[:0]
	w.next = 0
	// Do not carry a cold/constructor workset's map capacity into hot clears.
	w.pending = nil
}
func (n *Node) currentTreeNode() bool {
	for node := n; node.parent != nil; node = node.parent {
		node.noteWork(workPathInspections, 1)
		if node.parent.children[node.nodeName] != node {
			return false
		}
	}
	return true
}
func (n *Node) markBoundedDirty() {
	b := n.bounded
	for node := n; node != nil; node = node.parent {
		node.subtreeIsDirty = true
		b.touched.add(node)
	}
	// Conservative invalidation covers all fixed task-bearing components, including
	// ancestor pointers and lifecycle changes, without descending into DATA.
	for _, node := range b.slots {
		n.noteWork(workSlotVisits, 1)
		if node != nil {
			node.subtreeIsDirty = true
			b.touched.add(node)
		}
	}
}
func (n *Node) enqueueBoundedState() {
	if n.valueState >= valueStateNeedSerialize {
		n.bounded.bytes.add(n)
	}
	if n.valueState >= valueStateNeedSyncStructure && n.isComponent() {
		n.bounded.structure.add(n)
	}
}
func (n *Node) syncBoundedStructure() error {
	if err := n.validateRequiredEmptyMaps(); err != nil {
		return err
	}
	root := n.root()
	var deferred []*Node
	defer func() {
		for _, node := range deferred {
			root.bounded.structure.add(node)
		}
	}()
	for {
		for pending := range root.pendingDataMaps {
			if !n.containsBoundedNode(pending.owner) {
				continue
			}
			delete(root.pendingDataMaps, pending)
			if err := pending.sync(); err != nil {
				return err
			}
		}
		node := root.bounded.structure.pop()
		if node == nil {
			return nil
		}
		if !n.containsBoundedNode(node) {
			deferred = append(deferred, node)
			continue
		}
		if !node.currentTreeNode() || node.valueState < valueStateNeedSyncStructure {
			continue
		}
		if err := node.syncComponentFields(); err != nil {
			return err
		}
	}
}
func (n *Node) nodesToSerialize() iter.Seq2[[]string, *Node] {
	if n.bounded == nil {
		return n.andAllChildren()
	}
	return func(yield func([]string, *Node) bool) {
		var deferred []*Node
		defer func() {
			for _, node := range deferred {
				n.bounded.bytes.add(node)
			}
		}()
		base := len(n.path())
		for node := n.bounded.bytes.pop(); node != nil; node = n.bounded.bytes.pop() {
			if !n.containsBoundedNode(node) {
				deferred = append(deferred, node)
				continue
			}
			if node.currentTreeNode() && !yield(node.path()[base:], node) {
				return
			}
		}
	}
}

// componentNodes is only for task/component consumers. Full snapshots and
// replication export deliberately retain andAllChildren/snapshotInternal.
func (n *Node) componentNodes() iter.Seq2[[]string, *Node] {
	if n.bounded == nil {
		return n.andAllChildren()
	}
	return func(yield func([]string, *Node) bool) {
		base := n.path()
		for i, node := range n.bounded.slots {
			n.noteWork(workSlotVisits, 1)
			path := n.bounded.layout.components[i].path
			if len(path) < len(base) || !slices.Equal(path[:len(base)], base) {
				continue
			}
			if node != nil && node.currentTreeNode() && !yield(path[len(base):], node) {
				return
			}
		}
	}
}
func (n *Node) cleanupBoundedWork() {
	b := n.bounded
	for node := b.touched.pop(); node != nil; node = b.touched.pop() {
		node.subtreeIsDirty = false
	}
	clear(b.issued)
	b.totalRegular = 0
	b.totalImmediate = 0
	b.executed = 0
	b.structure.reset()
	b.bytes.reset()
}

// containsBoundedNode checks receiver scope without traversing DATA descendants.
func (n *Node) containsBoundedNode(node *Node) bool {
	for current := node; current != nil; current = current.parent {
		n.noteWork(workPathInspections, 1)
		if current == n {
			return true
		}
	}
	return false
}
