package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestNext_IncreaseTaskKey(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)

	r := NewRandomRange()

	taskKey := NewRandomKeyInRange(r)
	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetKey().Return(taskKey).Times(1)
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		require.Equal(t, r, paginationRange)
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	iterator := NewIterator(paginationFnProvider, r)
	require.Equal(t, r, iterator.Range())

	require.True(t, iterator.HasNext())
	task, err := iterator.Next()
	require.NoError(t, err)
	require.Equal(t, mockTask, task)

	require.Equal(t, NewRange(taskKey.Next(), r.ExclusiveMax), iterator.Range())

	require.False(t, iterator.HasNext())
}

func TestCanSplit(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()

	iterator := NewIterator(nil, r)
	require.Equal(t, r, iterator.Range())

	require.True(t, iterator.CanSplit(r.InclusiveMin))
	require.True(t, iterator.CanSplit(r.ExclusiveMax))
	require.True(t, iterator.CanSplit(NewRandomKeyInRange(r)))

	require.False(t, iterator.CanSplit(tasks.NewKey(
		r.InclusiveMin.FireTime,
		r.InclusiveMin.TaskID-1,
	)))
	require.False(t, iterator.CanSplit(tasks.NewKey(
		r.ExclusiveMax.FireTime.Add(time.Nanosecond),
		r.ExclusiveMax.TaskID,
	)))
}

func TestSplit(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		}
	}

	iterator := NewIterator(paginationFnProvider, r)
	require.Equal(t, r, iterator.Range())

	splitKey := NewRandomKeyInRange(r)

	leftIterator, rightIterator := iterator.Split(splitKey)
	require.Equal(t, NewRange(r.InclusiveMin, splitKey), leftIterator.Range())
	require.Equal(t, NewRange(splitKey, r.ExclusiveMax), rightIterator.Range())
	require.False(t, leftIterator.HasNext())
	require.False(t, leftIterator.HasNext())
}

func TestCanMerge(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()
	iterator := NewIterator(nil, r)

	incomingIterator := NewIterator(nil, r)
	require.True(t, iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(tasks.MinimumKey, r.InclusiveMin))
	require.True(t, iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(r.ExclusiveMax, tasks.MaximumKey))
	require.True(t, iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)))
	require.True(t, iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(NewRandomKeyInRange(r), tasks.MaximumKey))
	require.True(t, iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(tasks.MinimumKey, tasks.MaximumKey))
	require.True(t, iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	))
	require.False(t, iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	))
	require.False(t, iterator.CanMerge(incomingIterator))
}

func TestMerge(t *testing.T) {
	t.Parallel()

	r := NewRandomRange()

	numLoad := 0
	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			numLoad++
			return []tasks.Task{}, nil, nil
		}
	}

	iterator := NewIterator(paginationFnProvider, r)
	require.False(t, iterator.HasNext())

	incomingIterator := NewIterator(paginationFnProvider, r)
	mergedIterator := iterator.Merge(incomingIterator)
	require.Equal(t, r, mergedIterator.Range())
	require.False(t, mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(tasks.MinimumKey, r.InclusiveMin),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	require.Equal(t, NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedIterator.Range())
	require.False(t, mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(r.ExclusiveMax, tasks.MaximumKey),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	require.Equal(t, NewRange(r.InclusiveMin, tasks.MaximumKey), mergedIterator.Range())
	require.False(t, mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	require.Equal(t, NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedIterator.Range())
	require.False(t, mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(NewRandomKeyInRange(r), tasks.MaximumKey),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	require.Equal(t, NewRange(r.InclusiveMin, tasks.MaximumKey), mergedIterator.Range())
	require.False(t, mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(tasks.MinimumKey, tasks.MaximumKey),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	require.Equal(t, NewRange(tasks.MinimumKey, tasks.MaximumKey), mergedIterator.Range())
	require.False(t, mergedIterator.HasNext())

	// test if Merge returns a new iterator
	require.Equal(t, 7, numLoad)
}

func TestRemaining(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)

	r := NewRandomRange()
	r.InclusiveMin.FireTime = tasks.DefaultFireTime
	r.ExclusiveMax.FireTime = tasks.DefaultFireTime

	numLoad := 0
	taskKey := NewRandomKeyInRange(r)
	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetKey().Return(taskKey).Times(1)
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			numLoad++
			if paginationRange.ContainsKey(taskKey) {
				return []tasks.Task{mockTask}, nil, nil
			}
			return []tasks.Task{}, nil, nil
		}
	}

	iterator := NewIterator(paginationFnProvider, r)
	_, err := iterator.Next()
	require.NoError(t, err)
	require.False(t, iterator.HasNext())

	remaining := iterator.Remaining()
	require.Equal(t, iterator.Range(), remaining.Range())
	require.False(t, remaining.HasNext())

	// test if Remaining returns a new iterator
	require.Equal(t, 2, numLoad)
}
