package tasks

import (
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateKey_Valid(t *testing.T) {
	require.NoError(t, ValidateKey(Key{
		FireTime: time.Unix(0, 0),
		TaskID:   0,
	}))
	require.NoError(t, ValidateKey(Key{
		FireTime: time.Unix(0, math.MaxInt64),
		TaskID:   math.MaxInt64,
	}))
	require.NoError(t, ValidateKey(Key{
		FireTime: time.Unix(0, rand.Int63()),
		TaskID:   rand.Int63(),
	}))
}

func TestValidateKey_Invalid(t *testing.T) {
	require.Error(t, ValidateKey(Key{
		FireTime: time.Time{},
		TaskID:   0,
	}))
	require.Error(t, ValidateKey(Key{
		FireTime: time.Now(),
		TaskID:   -1,
	}))
}

func TestMinMaxKey(t *testing.T) {
	thisKey := NewKey(time.Unix(0, rand.Int63()), rand.Int63())
	thatKey := NewKey(time.Unix(0, rand.Int63()), rand.Int63())

	minKey := MinKey(thisKey, thatKey)
	require.True(t, minKey.CompareTo(thisKey) <= 0)
	require.True(t, minKey.CompareTo(thatKey) <= 0)

	maxKey := MaxKey(thisKey, thatKey)
	require.True(t, maxKey.CompareTo(thisKey) >= 0)
	require.True(t, maxKey.CompareTo(thisKey) >= 0)
}

func TestSort(t *testing.T) {
	numInstant := 256
	numTaskPerInstant := 16

	taskKeys := Keys{}
	for i := 0; i < numInstant; i++ {
		fireTime := time.Unix(0, rand.Int63())
		for j := 0; j < numTaskPerInstant; j++ {
			taskKeys = append(taskKeys, NewKey(fireTime, rand.Int63()))
		}
	}
	sort.Sort(taskKeys)

	for i := 1; i < numInstant*numTaskPerInstant; i++ {
		prev := taskKeys[i-1]
		next := taskKeys[i]

		if prev.FireTime.Before(next.FireTime) {
			// noop
		} else if prev.FireTime.Equal(next.FireTime) {
			require.True(t, prev.TaskID <= next.TaskID)
		} else {
			t.Fatalf("task keys are not sorted prev: %v, next: %v", prev, next)
		}
	}
}

func TestPrev(t *testing.T) {
	require.Equal(t, NewKey(time.Unix(0, 1), 0), NewKey(time.Unix(0, 1), 1).Prev())
	require.Equal(t, NewKey(time.Unix(0, 0), math.MaxInt64), NewKey(time.Unix(0, 1), 0).Prev())
	require.Equal(t, NewKey(time.Unix(0, 0), math.MaxInt64-1), NewKey(time.Unix(0, 0), math.MaxInt64).Prev())

	require.Equal(t, NewKey(time.Unix(0, math.MaxInt64), 0), NewKey(time.Unix(0, math.MaxInt64), 1).Prev())
	require.Equal(t, NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64), NewKey(time.Unix(0, math.MaxInt64), 0).Prev())
	require.Equal(t, NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64-1), NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64).Prev())
}

func TestNext(t *testing.T) {
	require.Equal(t, NewKey(time.Unix(0, 0), math.MaxInt64), NewKey(time.Unix(0, 0), math.MaxInt64-1).Next())
	require.Equal(t, NewKey(time.Unix(0, 1), 0), NewKey(time.Unix(0, 0), math.MaxInt64).Next())
	require.Equal(t, NewKey(time.Unix(0, 1), 1), NewKey(time.Unix(0, 1), 0).Next())

	require.Equal(t, NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64), NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64-1).Next())
	require.Equal(t, NewKey(time.Unix(0, math.MaxInt64), 0), NewKey(time.Unix(0, math.MaxInt64-1), math.MaxInt64).Next())
	require.Equal(t, NewKey(time.Unix(0, math.MaxInt64), 1), NewKey(time.Unix(0, math.MaxInt64), 0).Next())
}

func TestSub(t *testing.T) {
	require.Equal(t,
		NewKey(time.Unix(0, math.MaxInt64).UTC(), 1),
		MaximumKey.Sub(NewKey(time.Unix(0, 0).UTC(), math.MaxInt64-1)),
	)

	require.Equal(t,
		NewKey(time.Unix(0, math.MaxInt64).UTC(), 0),
		MaximumKey.Sub(NewKey(time.Unix(0, 0).UTC(), math.MaxInt64)),
	)

	require.Equal(t,
		NewKey(time.Unix(0, 0).UTC(), 1),
		NewKey(time.Unix(0, math.MaxInt64).UTC(), 1).Sub(NewKey(time.Unix(0, math.MaxInt64), 0)),
	)
}
