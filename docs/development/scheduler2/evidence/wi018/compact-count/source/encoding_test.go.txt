package queueindex

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountCanonicalIntegers(t *testing.T) {
	t.Parallel()
	for _, value := range []uint64{0, 1, 127, 128, 16383, 16384, math.MaxUint64} {
		wire := binary.AppendUvarint(nil, value)
		actual, size, err := canonicalUint(wire)
		require.NoError(t, err)
		require.Equal(t, value, actual)
		require.Equal(t, len(wire), size)
	}
	for _, wire := range [][]byte{nil, {0x80}, {0x80, 0}, {0x81, 0}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}, {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0}} {
		_, _, err := canonicalUint(wire)
		require.Error(t, err)
	}
}

func TestCountCompactEncodingBoundaries(t *testing.T) {
	t.Parallel()
	for _, count := range []uint64{1, 127, 128, math.MaxUint64} {
		for _, refs := range []uint64{1, 127, 128, math.MaxUint64} {
			entry := countEntry{Node: countLeaf(CountKey{1}, count), Refs: refs}
			id := CountRoot(sha256.Sum256(entry.Node.encode()))
			decoded, err := decodeCount(id, entry.encode())
			require.NoError(t, err)
			require.Equal(t, entry, decoded)
		}
	}
	node := countLeaf(CountKey{1}, 1)
	valid := node.encode()
	id := CountRoot(sha256.Sum256(valid))
	for _, refs := range [][]byte{{0}, {0x81, 0}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2}} {
		wire := append([]byte{6}, refs...)
		wire = append(wire, valid...)
		_, err := decodeCount(id, wire)
		require.Error(t, err)
	}
	// Hash matching alone cannot authorize noncanonical integer or trailing bytes.
	for _, suffix := range [][]byte{{0x81, 0}, {0}, {0x80}, {1, 0}} {
		malformed := append([]byte(nil), valid[:14]...)
		malformed = append(malformed, suffix...)
		hash := CountRoot(sha256.Sum256(malformed))
		_, err := decodeCount(hash, append([]byte{6, 1}, malformed...))
		require.Error(t, err)
	}
}
