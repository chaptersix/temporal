package shard

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

func TestTrackAndMinTaskKey(t *testing.T) {
	tracker := newTaskRequestTracker(tasks.NewDefaultTaskCategoryRegistry())
	now := time.Now()

	_ = tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(123),
			tasks.NewImmediateKey(125),
		},
		tasks.CategoryTimer: {
			tasks.NewKey(now, 124),
			tasks.NewKey(now.Add(time.Minute), 122),
		},
	}))
	assertMinTaskKey(t, tracker, tasks.CategoryTransfer, tasks.NewImmediateKey(123))
	assertMinTaskKey(t, tracker, tasks.CategoryTimer, tasks.NewKey(now, 124))

	_ = tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(130),
		},
		tasks.CategoryTimer: {
			tasks.NewKey(now.Add(-time.Minute), 131),
		},
	}))
	assertMinTaskKey(t, tracker, tasks.CategoryTransfer, tasks.NewImmediateKey(123))
	assertMinTaskKey(t, tracker, tasks.CategoryTimer, tasks.NewKey(now.Add(-time.Minute), 131))

	_, ok := tracker.minTaskKey(tasks.CategoryVisibility)
	require.False(t, ok)
}

func TestRequestCompletion(t *testing.T) {
	tracker := newTaskRequestTracker(tasks.NewDefaultTaskCategoryRegistry())
	completionFunc1 := tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(123),
			tasks.NewImmediateKey(125),
		},
	}))
	completionFunc2 := tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(122),
		},
	}))
	completionFunc3 := tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(127),
		},
	}))
	assertMinTaskKey(t, tracker, tasks.CategoryTransfer, tasks.NewImmediateKey(122))

	completionFunc2(nil)
	assertMinTaskKey(t, tracker, tasks.CategoryTransfer, tasks.NewImmediateKey(123))

	completionFunc3(serviceerror.NewNotFound("not found error guarantees task is not inserted"))
	assertMinTaskKey(t, tracker, tasks.CategoryTransfer, tasks.NewImmediateKey(123))

	completionFunc1(errors.New("random error means task may still be inserted in the future"))
	assertMinTaskKey(t, tracker, tasks.CategoryTransfer, tasks.NewImmediateKey(123))

	tracker.drain()
}

func TestDrain(t *testing.T) {
	tracker := newTaskRequestTracker(tasks.NewDefaultTaskCategoryRegistry())
	// drain should not block if there is no inflight request
	tracker.drain()

	completionFunc1 := tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(123),
		},
	}))
	completionFunc2 := tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(122),
		},
	}))
	completionFunc3 := tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(127),
		},
	}))

	for _, completionFn := range []taskRequestCompletionFn{
		completionFunc1,
		completionFunc2,
		completionFunc3,
	} {
		go func(completionFn taskRequestCompletionFn) {
			completionFn(nil)
		}(completionFn)
	}

	tracker.drain()
}

func TestClear(t *testing.T) {
	tracker := newTaskRequestTracker(tasks.NewDefaultTaskCategoryRegistry())
	_ = tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(123),
			tasks.NewImmediateKey(125),
		},
	}))
	completionFn := tracker.track(convertKeysToTasks(t, map[tasks.Category][]tasks.Key{
		tasks.CategoryTransfer: {
			tasks.NewImmediateKey(122),
		},
	}))
	completionFn(errors.New("some random error"))
	assertMinTaskKey(t, tracker, tasks.CategoryTransfer, tasks.NewImmediateKey(122))

	tracker.clear()
	_, ok := tracker.minTaskKey(tasks.CategoryTransfer)
	require.False(t, ok)
	tracker.drain()
}

func assertMinTaskKey(
	t *testing.T,
	tracker *taskRequestTracker,
	category tasks.Category,
	expectedKey tasks.Key,
) {
	actualKey, ok := tracker.minTaskKey(category)
	require.True(t, ok)
	require.Zero(t, expectedKey.CompareTo(actualKey))
}

func convertKeysToTasks(
	t *testing.T,
	keysByCategory map[tasks.Category][]tasks.Key,
) map[tasks.Category][]tasks.Task {
	tasksByCategory := make(map[tasks.Category][]tasks.Task)
	for category, keys := range keysByCategory {
		tasksByCategory[category] = make([]tasks.Task, 0, len(keys))
		for _, key := range keys {
			fakeTask := tasks.NewFakeTask(
				tests.WorkflowKey,
				category,
				time.Time{},
			)
			fakeTask.SetTaskID(key.TaskID)
			fakeTask.SetVisibilityTime(key.FireTime)
			tasksByCategory[category] = append(tasksByCategory[category], fakeTask)
		}
	}

	return tasksByCategory
}
