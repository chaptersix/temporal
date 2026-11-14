package queueindex

// SequenceAtOrAfter returns the lowest sequence at least lower. It reads only
// bounded outer-index paths, without changing pages or count-root ownership.
func (b *CountBatch) SequenceAtOrAfter(index SequenceIndex, lower uint64) (SequenceEntry, bool, error) {
	root, err := b.sequenceRoot(index)
	if err != nil {
		return SequenceEntry{}, false, err
	}
	if root == (sequenceID{}) {
		return SequenceEntry{}, false, nil
	}
	node, err := b.sequenceNode(index, root)
	if err != nil {
		return SequenceEntry{}, false, err
	}
	var fallback sequenceNode
	haveFallback := false
	for node.Bit < 64 {
		left, right, err := b.sequenceChildren(index, node)
		if err != nil {
			return SequenceEntry{}, false, err
		}
		if lower >= right.Min {
			node = right
		} else {
			node = left
			fallback = right
			haveFallback = true
		}
	}
	if node.Min >= lower {
		return SequenceEntry{Sequence: node.Min, Time: node.Time}, true, nil
	}
	if !haveFallback {
		return SequenceEntry{}, false, nil
	}
	// The latest remembered right subtree is the smallest candidate above lower.
	// Its minimum already exceeds lower, so only its validated left path is needed.
	node = fallback
	for node.Bit < 64 {
		left, _, err := b.sequenceChildren(index, node)
		if err != nil {
			return SequenceEntry{}, false, err
		}
		node = left
	}
	return SequenceEntry{Sequence: node.Min, Time: node.Time}, true, nil
}
