package queueindex

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeKeyChronologicalOrder(t *testing.T) {
	t.Parallel()
	instants := []time.Time{
		time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(100, 1, 1, 0, 0, 0, 123456789, time.UTC),
		time.Unix(-1, 0),
		time.Unix(0, -1),
		time.Unix(0, 0),
		time.Unix(0, 1),
		time.Unix(1, 0),
		time.Date(2500, 1, 1, 0, 0, 0, 123456789, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
	}
	var previous TimeKey
	for i, instant := range instants {
		for j, sequence := range []uint64{0, 1, math.MaxUint64} {
			key := NewTimeKey(instant, sequence)
			require.True(t, instant.Equal(key.Time()))
			require.Equal(t, sequence, key.Sequence())
			if i != 0 || j != 0 {
				require.Negative(t, bytes.Compare(previous[:], key[:]), "chronological ordering must dominate even maximal prior sequence")
			}
			previous = key
		}
	}
}

func TestTimeKeyNormalizesInstant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		at   time.Time
	}{
		{"negative_nanosecond", time.Unix(0, -1)},
		{"normalized_nanosecond", time.Unix(-1, 999999999)},
		{"different_location", time.Unix(0, -1).In(time.FixedZone("offset", 3600))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, TimeKey{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x3b, 0x9a, 0xc9, 0xff, 0, 0, 0, 0, 0, 0, 0, 7}, NewTimeKey(tc.at, 7))
		})
	}
}
