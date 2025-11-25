package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/tasks"
)

func TestNewRange_Invalid(t *testing.T) {
	t.Parallel()

	minKey := NewRandomKey()
	maxKey := tasks.NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()-1),
		minKey.TaskID,
	)
	require.Panics(t, func() { NewRange(minKey, maxKey) })

	maxKey = tasks.NewKey(minKey.FireTime, minKey.TaskID-1)
	require.Panics(t, func() { NewRange(minKey, maxKey) })
}

func TestNewRange_Valid(t *testing.T) {
	t.Parallel()

	minKey := NewRandomKey()
	_ = NewRange(minKey, minKey)

	maxKey := tasks.NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()+1),
		minKey.TaskID,
	)
	_ = NewRange(minKey, maxKey)

	maxKey = tasks.NewKey(minKey.FireTime, minKey.TaskID+1)
	_ = NewRange(minKey, maxKey)

	maxKey = tasks.NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()+1),
		minKey.TaskID-1,
	)
	_ = NewRange(minKey, maxKey)
}

func TestRange_IsEmpty(t *testing.T) {
	t.Parallel()

	minKey := NewRandomKey()
	r := NewRange(minKey, minKey)
	require.True(t, r.IsEmpty())

	maxKey := tasks.NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()+1),
		minKey.TaskID,
	)
	r = NewRange(minKey, maxKey)
	require.False(t, r.IsEmpty())

	maxKey = tasks.NewKey(minKey.FireTime, minKey.TaskID+1)
	r = NewRange(minKey, maxKey)
	require.False(t, r.IsEmpty())

	maxKey = tasks.NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()+1),
		minKey.TaskID-1,
	)
	r = NewRange(minKey, maxKey)
	require.False(t, r.IsEmpty())
}

func TestRange_ContainsKey_EmptyRange(t *testing.T) {
	t.Parallel()

	key := NewRandomKey()
	r := NewRange(key, key)

	testKey := key
	require.False(t, r.ContainsKey(testKey))

	testKey = tasks.NewKey(key.FireTime.Add(time.Nanosecond), key.TaskID)
	require.False(t, r.ContainsKey(testKey))

	testKey = tasks.NewKey(key.FireTime.Add(-time.Nanosecond), key.TaskID)
	require.False(t, r.ContainsKey(testKey))

	testKey = tasks.NewKey(key.FireTime, key.TaskID-1)
	require.False(t, r.ContainsKey(testKey))

	testKey = tasks.NewKey(key.FireTime, key.TaskID+1)
	require.False(t, r.ContainsKey(testKey))
}

func TestRange_ContainsKey_NonEmptyRange(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()

	testKey := r.InclusiveMin
	require.True(t, r.ContainsKey(testKey))

	testKey = r.ExclusiveMax
	require.False(t, r.ContainsKey(testKey))

	testKey = tasks.NewKey(r.InclusiveMin.FireTime.Add(-time.Nanosecond), r.InclusiveMin.TaskID)
	require.False(t, r.ContainsKey(testKey))

	testKey = tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1)
	require.False(t, r.ContainsKey(testKey))

	testKey = tasks.NewKey(r.ExclusiveMax.FireTime.Add(time.Nanosecond), r.ExclusiveMax.TaskID)
	require.False(t, r.ContainsKey(testKey))

	testKey = tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1)
	require.False(t, r.ContainsKey(testKey))

	for i := 0; i != 1000; i++ {
		require.True(t, r.ContainsKey(NewRandomKeyInRange(r)))
	}
}

func TestRange_ContainsRange_EmptyRange(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	require.True(t, r.ContainsRange(NewRange(r.InclusiveMin, r.InclusiveMin)))
	require.True(t, r.ContainsRange(NewRange(r.ExclusiveMax, r.ExclusiveMax)))

	key := NewRandomKey()
	r = NewRange(key, key)
	require.True(t, r.ContainsRange(r))

	require.False(t, r.ContainsRange(NewRandomRange()))
}

func TestRange_ContainsRange_NonEmptyRange(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()

	testRange := r
	require.True(t, r.ContainsRange(testRange))

	testRange = NewRange(
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID+1),
		r.ExclusiveMax,
	)
	require.True(t, r.ContainsRange(testRange))
	require.False(t, testRange.ContainsRange(r))

	testRange = NewRange(
		r.InclusiveMin,
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
	)
	require.False(t, r.ContainsRange(testRange))
	require.True(t, testRange.ContainsRange(r))

	testRange = NewRange(
		NewRandomKeyInRange(r),
		r.ExclusiveMax,
	)
	require.True(t, r.ContainsRange(testRange))
	require.False(t, testRange.ContainsRange(r))

	testRange = NewRange(
		r.InclusiveMin,
		NewRandomKeyInRange(r),
	)
	require.True(t, r.ContainsRange(testRange))
	require.False(t, testRange.ContainsRange(r))
}

func TestRange_CanSplit(t *testing.T) {
	t.Parallel()

	key := NewRandomKey()
	ranges := []Range{
		NewRandomRange(),
		NewRange(key, key),
	}

	for _, r := range ranges {
		testKey := r.InclusiveMin
		require.True(t, r.CanSplit(testKey))

		testKey = r.ExclusiveMax
		require.True(t, r.CanSplit(testKey))

		if !r.IsEmpty() {
			for i := 0; i != 1000; i++ {
				require.True(t, r.CanSplit(NewRandomKeyInRange(r)))
			}
		}
	}
}

func TestRange_CanMerge(t *testing.T) {
	t.Parallel()

	key := NewRandomKey()
	ranges := []Range{
		NewRandomRange(),
		NewRange(key, key),
	}

	for _, r := range ranges {
		if !r.IsEmpty() {
			testRange := NewRange(
				tasks.MinimumKey,
				NewRandomKeyInRange(r),
			)
			require.True(t, r.CanMerge(testRange))
			require.True(t, testRange.CanMerge(r))

			testRange = NewRange(
				NewRandomKeyInRange(r),
				tasks.MaximumKey,
			)
			require.True(t, r.CanMerge(testRange))
			require.True(t, testRange.CanMerge(r))
		}

		testRange := NewRange(
			tasks.MinimumKey,
			tasks.MaximumKey,
		)
		require.True(t, r.CanMerge(testRange))
		require.True(t, testRange.CanMerge(r))

		testRange = NewRange(
			tasks.MinimumKey,
			r.InclusiveMin,
		)
		require.True(t, r.CanMerge(testRange))
		require.True(t, testRange.CanMerge(r))

		testRange = NewRange(
			r.ExclusiveMax,
			tasks.MaximumKey,
		)
		require.True(t, r.CanMerge(testRange))
		require.True(t, testRange.CanMerge(r))

		testRange = NewRange(
			tasks.MinimumKey,
			tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
		)
		require.False(t, r.CanMerge(testRange))
		require.False(t, testRange.CanMerge(r))

		testRange = NewRange(
			tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
			tasks.MaximumKey,
		)
		require.False(t, r.CanMerge(testRange))
		require.False(t, testRange.CanMerge(r))
	}
}

func TestRange_Split(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	splitKey := NewRandomKeyInRange(r)

	left, right := r.Split(splitKey)
	require.True(t, left.Equals(NewRange(r.InclusiveMin, splitKey)))
	require.True(t, right.Equals(NewRange(splitKey, r.ExclusiveMax)))
}

func TestRange_Merge(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()

	testRange := NewRange(
		tasks.MinimumKey,
		NewRandomKeyInRange(r),
	)
	mergedRange := r.Merge(testRange)
	require.True(t, mergedRange.Equals(testRange.Merge(r)))
	require.True(t, mergedRange.Equals(NewRange(tasks.MinimumKey, r.ExclusiveMax)))

	testRange = NewRange(
		NewRandomKeyInRange(r),
		tasks.MaximumKey,
	)
	mergedRange = r.Merge(testRange)
	require.True(t, mergedRange.Equals(testRange.Merge(r)))
	require.True(t, mergedRange.Equals(NewRange(r.InclusiveMin, tasks.MaximumKey)))

	testRange = NewRange(tasks.MinimumKey, tasks.MaximumKey)
	mergedRange = r.Merge(testRange)
	require.True(t, mergedRange.Equals(testRange.Merge(r)))
	require.True(t, mergedRange.Equals(NewRange(tasks.MinimumKey, tasks.MaximumKey)))

	testRange = NewRange(tasks.MinimumKey, r.InclusiveMin)
	mergedRange = r.Merge(testRange)
	require.True(t, mergedRange.Equals(testRange.Merge(r)))
	require.True(t, mergedRange.Equals(NewRange(tasks.MinimumKey, r.ExclusiveMax)))

	testRange = NewRange(r.ExclusiveMax, tasks.MaximumKey)
	mergedRange = r.Merge(testRange)
	require.True(t, mergedRange.Equals(testRange.Merge(r)))
	require.True(t, mergedRange.Equals(NewRange(r.InclusiveMin, tasks.MaximumKey)))
}
