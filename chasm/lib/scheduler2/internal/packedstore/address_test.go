package packedstore

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackedAddressCanonicalIdentity(t *testing.T) {
	ids := []ID{{}, {1}, {1, 0, 1}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}}
	rng := rand.New(rand.NewSource(73))
	for range 1000 {
		var id ID
		_, err := rng.Read(id[:])
		require.NoError(t, err)
		ids = append(ids, id)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		for _, ordinal := range []uint64{0, 1, 15, 16, 255, 256, math.MaxUint64} {
			encoded := key(id, ordinal)
			require.False(t, seen[encoded])
			seen[encoded] = true
			decoded, n, err := decodeKey(encoded)
			require.NoError(t, err)
			require.Equal(t, id, decoded)
			require.Equal(t, ordinal, n)
			require.LessOrEqual(t, len(encoded), 2+22+1+16)
		}
	}
	for _, encoded := range []string{"p/AA/1", "p/AQ/01", "p/AQ/FF", "p/AQ/+1", "p/AQ/-1", "p/AQ/10000000000000000", "p/AQ==/1", "p/AQ/1/2", "p/AAAAAAAAAAAAAAAAAAAAAAA/1"} {
		_, _, err := decodeKey(encoded)
		require.Error(t, err)
	}
}
