package occurrence

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
)

func seedReader(seed []byte) func(uint64) (SeedWindow, error) {
	return func(offset uint64) (SeedWindow, error) {
		var p SeedWindow
		if offset > uint64(len(seed)) {
			return p, errors.New("seed offset")
		}
		p.Count = uint8(copy(p.Bytes[:], seed[offset:]))
		return p, nil
	}
}

func TestPreparedJitterReference(t *testing.T) {
	t.Parallel()
	for _, length := range []int{0, 1, 4, 5, 8, 9, 10, 11, 19, 20, 24, 25, 39, 40, 41, 100, 1001, 10000} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			seed := make([]byte, length)
			for i := range seed {
				seed[i] = byte(i*137 + 19)
			}
			nominal := time.Date(2024, 11, 3, 6, 30, 0, 0, time.UTC)
			b, err := nominal.MarshalBinary()
			require.NoError(t, err)
			expected := farm.Fingerprint32(append(b, seed...))
			var state JitterHashState
			for !state.Done {
				before := state
				failed, err := advanceJitterHash(state, nominal, uint64(length), func(uint64) (SeedWindow, error) { return SeedWindow{}, errors.New("fault") })
				if err != nil {
					require.Equal(t, before, failed)
				}
				state, err = advanceJitterHash(state, nominal, uint64(length), seedReader(seed))
				require.NoError(t, err)
			}
			require.Equal(t, expected, state.Hash)
		})
	}
}
