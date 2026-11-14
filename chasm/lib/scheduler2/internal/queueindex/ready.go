package queueindex

import (
	"bytes"
	"errors"
)

// SequenceEarliest returns the smallest chronological time and, among records
// at that time, the smallest sequence. Callers compare Time with their current
// exact time to distinguish a due record from the next future wake. This query
// reads fixed-width paths only and does not acquire or release index ownership.
func (b *CountBatch) SequenceEarliest(index SequenceIndex) (SequenceEntry, bool, error) {
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
	earliest, err := b.sequenceMinimum(node)
	if err != nil {
		return SequenceEntry{}, false, err
	}
	for node.Bit < 64 {
		left, right, err := b.sequenceChildren(index, node)
		if err != nil {
			return SequenceEntry{}, false, err
		}
		leftTime, err := b.sequenceMinimum(left)
		if err != nil {
			return SequenceEntry{}, false, err
		}
		rightTime, err := b.sequenceMinimum(right)
		if err != nil {
			return SequenceEntry{}, false, err
		}
		minimum := leftTime
		if bytes.Compare(rightTime[:], leftTime[:]) < 0 {
			minimum = rightTime
		}
		if minimum != earliest {
			return SequenceEntry{}, false, b.fail(errors.New("sequence chronological minimum mismatch"))
		}
		if leftTime == earliest {
			node = left
		} else {
			node = right
		}
	}
	if node.Time != earliest {
		return SequenceEntry{}, false, b.fail(errors.New("sequence leaf time mismatch"))
	}
	return SequenceEntry{Sequence: node.Min, Time: node.Time}, true, nil
}

func (b *CountBatch) sequenceMinimum(node sequenceNode) (CountKey, error) {
	if node.Bit == 64 {
		return node.Time, nil
	}
	entry, err := b.tx.read(node.Summary)
	if err != nil {
		return CountKey{}, b.fail(err)
	}
	// Following the validated minimum path avoids trusting a root's cached key
	// without establishing its reachable bucket. Sibling relationships and counts
	// are checked at each touched branch; unrelated descendants are not scanned.
	for entry.Node.Bit < 96 {
		left, _, err := validateCountBranch(entry.Node, b.tx.read)
		if err != nil {
			return CountKey{}, b.fail(err)
		}
		entry = left
	}
	return entry.Node.Key, nil
}
